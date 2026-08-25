// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use matrixraft::benchmark::{
    matrixraft_assert_production_baseline_raft_parity,
    matrixraft_assert_production_baseline_raft_summary,
    matrixraft_baseline_raft_benchmark_evidence,
    matrixraft_baseline_raft_benchmark_failure_summary, matrixraft_find_baseline_raft_harness,
    matrixraft_find_or_build_baseline_raft_harness,
    matrixraft_probe_baseline_raft_native_benchmark, matrixraft_run_baseline_raft_parity_benchmark,
    matrixraft_validate_production_baseline_raft_benchmark_options, RustRaftBenchmarkEngine,
    RustRaftBenchmarkEngineSource, RustRaftBenchmarkHarnessKind, RustRaftBenchmarkOptions,
    RustRaftBenchmarkRunner, RustRaftBenchmarkWorkload, RustRaftExternalBaselineRaftRunner,
    RustRaftRuntimeBenchmarkRunner, MATRIXRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT,
    MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
    MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES,
};
use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-real-baseline_raft-{name}-{}-{nonce}",
        std::process::id()
    ))
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_debug_benchmark_environment() {
    let root = temp_dir("debug-benchmark-environment");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    assert!(report
        .environment_fingerprint
        .contains("debug_assertions=true"));
    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let report_path = root.join("report.json");
    let summary_path = root.join("summary.json");
    fs::write(
        &report_path,
        serde_json::to_vec_pretty(&report).expect("serialize report"),
    )
    .expect("write report");
    fs::write(
        &summary_path,
        serde_json::to_vec_pretty(&summary).expect("serialize summary"),
    )
    .expect("write summary");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report_path)
        .arg("--summary")
        .arg(&summary_path)
        .arg("--max-age-seconds")
        .arg("86400")
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:report_environment_debug_assertions_enabled"));
    assert!(stderr.contains("benchmark:summary_environment_debug_assertions_enabled"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_stale_artifacts_before_parsing() {
    let root = temp_dir("stale-artifacts");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("summary.json");
    fs::write(&report, "{}").expect("report");
    fs::write(&summary, "{}").expect("summary");

    let touch = Command::new("touch")
        .arg("-d")
        .arg("@1")
        .arg(&report)
        .arg(&summary)
        .output()
        .expect("set stale mtimes");
    assert!(
        touch.status.success(),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&touch.stdout),
        String::from_utf8_lossy(&touch.stderr)
    );

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .arg("--max-age-seconds")
        .arg("1")
        .output()
        .expect("run artifact verifier");
    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_stale:report"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_invalid_max_age_before_parsing() {
    let root = temp_dir("invalid-artifact-age");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("summary.json");
    fs::write(&report, "{}").expect("report");
    fs::write(&summary, "{}").expect("summary");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .arg("--max-age-seconds")
        .arg("0")
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_max_artifact_age_seconds:0"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_example_rejects_invalid_env_max_age_before_parsing() {
    let root = temp_dir("invalid-artifact-age-env");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("summary.json");
    fs::write(&report, "{}").expect("report");
    fs::write(&summary, "{}").expect("summary");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--example")
        .arg("baseline_raft_parity_verify")
        .arg("--")
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .env("RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS", "bogus")
        .output()
        .expect("run direct artifact verifier example");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_max_artifact_age_seconds:bogus"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_path_collision_before_parsing() {
    let root = temp_dir("path-collision");
    fs::create_dir_all(&root).expect("root");
    let artifact = root.join("artifact.json");
    fs::write(&artifact, "{}").expect("artifact");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&artifact)
        .arg("--summary")
        .arg(&artifact)
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_path_collision:report_summary"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_missing_report_before_parsing() {
    let root = temp_dir("missing-report");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("missing-report.json");
    let summary = root.join("summary.json");
    fs::write(&summary, "{}").expect("summary");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_missing:report"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_missing_summary_before_parsing() {
    let root = temp_dir("missing-summary");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("missing-summary.json");
    fs::write(&report, "{}").expect("report");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_missing:summary"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_empty_artifacts_before_parsing() {
    let root = temp_dir("empty-artifact");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("summary.json");
    fs::write(&report, "").expect("empty report");
    fs::write(&summary, "{}").expect("summary");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_empty:report"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_rejects_directory_artifacts_before_parsing() {
    let root = temp_dir("directory-artifact");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report-dir");
    let summary = root.join("summary.json");
    fs::create_dir_all(&report).expect("report dir");
    fs::write(&summary, "{}").expect("summary");

    let verify_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("verify_baseline_raft_benchmark_artifacts.sh");
    let output = Command::new("bash")
        .arg(verify_script)
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run artifact verifier");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_not_file:report"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_example_rejects_path_collision_before_parsing() {
    let root = temp_dir("path-collision-example");
    fs::create_dir_all(&root).expect("root");
    let artifact = root.join("artifact.json");
    fs::write(&artifact, "{}").expect("artifact");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--example")
        .arg("baseline_raft_parity_verify")
        .arg("--")
        .arg("--report")
        .arg(&artifact)
        .arg("--summary")
        .arg(&artifact)
        .output()
        .expect("run direct artifact verifier example");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_path_collision:report_summary"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_example_rejects_directory_artifacts_before_parsing() {
    let root = temp_dir("directory-artifact-example");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report-dir");
    let summary = root.join("summary.json");
    fs::create_dir_all(&report).expect("report dir");
    fs::write(&summary, "{}").expect("summary");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--example")
        .arg("baseline_raft_parity_verify")
        .arg("--")
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run direct artifact verifier example");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_not_file:report"));
    assert!(!stderr.contains("failed to parse report"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_artifact_verifier_example_rejects_empty_artifacts_before_parsing() {
    let root = temp_dir("empty-artifact-example");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary = root.join("summary.json");
    fs::write(&report, "{}").expect("report");
    fs::write(&summary, "").expect("empty summary");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--example")
        .arg("baseline_raft_parity_verify")
        .arg("--")
        .arg("--report")
        .arg(&report)
        .arg("--summary")
        .arg(&summary)
        .output()
        .expect("run direct artifact verifier example");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_empty:summary"));
    assert!(!stderr.contains("failed to parse summary"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn baseline_raft_benchmark_script_verifies_successful_artifacts_before_exit() {
    let benchmark_script = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("baseline_raft_vs_rustraft_benchmark.sh"),
    )
    .expect("read benchmark script");

    assert!(benchmark_script.contains("verifier_status=0"));
    assert!(benchmark_script.contains("if [[ -f \"$out_path\" && -f \"$summary_path\" ]]; then"));
    assert!(benchmark_script.contains("if [[ \"$benchmark_status\" -eq 0 ]]; then"));
    assert!(benchmark_script.contains("benchmark:artifact_missing_after_benchmark:report"));
    assert!(benchmark_script.contains("benchmark:artifact_missing_after_benchmark:summary"));
    assert!(benchmark_script.contains("verify_baseline_raft_benchmark_artifacts.sh"));
    assert!(benchmark_script.contains("rm -f -- \"$out_path\" \"$summary_path\""));
    assert!(benchmark_script.contains("out_dir=\"$(dirname -- \"$out_path\")\""));
    assert!(benchmark_script.contains("out_file=\"$(basename -- \"$out_path\")\""));
    assert!(benchmark_script.contains("tmp_report=\"$(mktemp \"$out_dir/.${out_file}.XXXXXX\")\""));
    assert!(benchmark_script.contains("fsync_file()"));
    assert!(benchmark_script.contains("fsync_parent_dir()"));
    assert!(benchmark_script.contains("os.fsync(file.fileno())"));
    assert!(benchmark_script.contains("os.fsync(fd)"));
    assert!(benchmark_script.contains("fsync_file \"$tmp_report\""));
    assert!(benchmark_script.contains("mv -f -- \"$tmp_report\" \"$out_path\""));
    assert!(benchmark_script.contains("fsync_file \"$out_path\""));
    assert!(benchmark_script.contains("fsync_parent_dir \"$out_path\""));
    assert!(benchmark_script.contains("fsync_file \"$summary_path\""));
    assert!(benchmark_script.contains("fsync_parent_dir \"$summary_path\""));
    assert!(benchmark_script.contains("--report \"$out_path\""));
    assert!(benchmark_script.contains("--summary \"$summary_path\""));
    assert!(benchmark_script.contains(
        "benchmark_max_artifact_age_seconds=\"${RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS:-86400}\""
    ));
    assert!(
        benchmark_script.contains("benchmark_node_count=\"${RUSTRAFT_BENCHMARK_NODE_COUNT:-5}\"")
    );
    assert!(benchmark_script.contains(
        "benchmark_payload_size_bytes=\"${RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES:-4096}\""
    ));
    assert!(benchmark_script.contains(
        "benchmark_pass_tolerance_percent=\"${RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT:-10.0}\""
    ));
    assert!(benchmark_script.contains(
        "require_minimum_integer \"iterations_per_workload_below_production_min\" \"$benchmark_iterations\" 128"
    ));
    assert!(benchmark_script.contains(
        "require_minimum_integer \"node_count_below_production_scale\" \"$benchmark_node_count\" 5"
    ));
    assert!(benchmark_script.contains(
        "require_minimum_integer \"batch_size_below_production_min\" \"$benchmark_batch_size\" 2"
    ));
    assert!(benchmark_script.contains(
        "require_minimum_integer \"payload_size_below_production_min\" \"$benchmark_payload_size_bytes\" 4096"
    ));
    assert!(benchmark_script.contains(
        "require_maximum_decimal \"pass_tolerance_above_production_max\" \"$benchmark_pass_tolerance_percent\" 10.0"
    ));
    assert!(benchmark_script.contains("--node-count)"));
    assert!(benchmark_script.contains("--pass-tolerance-percent)"));
    assert!(benchmark_script.contains("RUSTRAFT_BENCHMARK_NODE_COUNT=\"$benchmark_node_count\""));
    assert!(benchmark_script.contains(
        "RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT=\"$benchmark_pass_tolerance_percent\""
    ));
    assert!(benchmark_script.contains("--max-age-seconds \"$benchmark_max_artifact_age_seconds\""));
    assert!(benchmark_script.contains("cargo_profile=(--release)"));
    assert!(benchmark_script.contains("build_profile=release"));
    assert!(benchmark_script.contains("verifier_profile_arg=--release"));
    assert!(benchmark_script.contains("--debug)"));
    assert!(benchmark_script.contains("build_profile=debug"));
    assert!(benchmark_script.contains("verifier_profile_arg=--debug"));
    assert!(benchmark_script.contains("if [[ \"$benchmark_status\" -ne 0 ]]; then"));
    assert!(benchmark_script.contains("exit \"$benchmark_status\""));
    assert!(benchmark_script.contains("exit \"$verifier_status\""));
    assert!(benchmark_script.contains("\"$verifier_profile_arg\""));
    assert_eq!(
        benchmark_script
            .matches("echo \"BaselineRaft benchmark harness: $baseline_raft_bin\"")
            .count(),
        1
    );
}

#[test]
fn benchmark_artifact_verifier_script_rejects_non_file_artifacts() {
    let verifier_script = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("verify_baseline_raft_benchmark_artifacts.sh"),
    )
    .expect("read verifier script");

    assert!(verifier_script.contains("benchmark:artifact_not_file:report"));
    assert!(verifier_script.contains("benchmark:artifact_not_file:summary"));
    assert!(verifier_script.contains("benchmark:artifact_empty:report"));
    assert!(verifier_script.contains("benchmark:artifact_empty:summary"));
    assert!(verifier_script.contains("benchmark:artifact_path_collision:report_summary"));
    assert!(verifier_script.contains("[[ \"$report_path\" == \"$summary_path\" ]]"));
    assert!(verifier_script.contains("[[ ! -f \"$report_path\" ]]"));
    assert!(verifier_script.contains("[[ ! -f \"$summary_path\" ]]"));
    assert!(verifier_script.contains("[[ ! -s \"$report_path\" ]]"));
    assert!(verifier_script.contains("[[ ! -s \"$summary_path\" ]]"));
}

#[test]
fn production_benchmark_options_validator_rejects_non_release_scale_inputs() {
    let options = RustRaftBenchmarkOptions {
        node_count: 3,
        iterations_per_workload: 0,
        batch_size: 1,
        payload_size_bytes: 1024,
        pass_tolerance_percent: MATRIXRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT + 0.1,
    };

    let error = matrixraft_validate_production_baseline_raft_benchmark_options(&options)
        .expect_err("invalid production options must fail preflight");
    assert!(error.contains("benchmark:node_count_below_production_scale:3:5"));
    assert!(error.contains("benchmark:invalid_iterations_per_workload:0"));
    assert!(error.contains("benchmark:batch_size_below_production_min:1:2"));
    assert!(error.contains("benchmark:payload_size_below_production_min:1024:4096"));
    assert!(error.contains("benchmark:invalid_pass_tolerance_percent:10.100"));
}

#[test]
fn direct_baseline_raft_benchmark_example_preflights_options_before_harness_lookup() {
    let example = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("examples")
            .join("baseline_raft_parity_benchmark.rs"),
    )
    .expect("read benchmark example");

    let preflight = example
        .find("matrixraft_validate_production_baseline_raft_benchmark_options(&options)")
        .expect("example should preflight production options");
    let harness_lookup = example
        .find("let baseline_raft_root =")
        .expect("example should look up BaselineRaft after option parsing");
    assert!(
        preflight < harness_lookup,
        "production option preflight must happen before BaselineRaft harness lookup"
    );
    assert!(example.contains("BaselineRaft parity benchmark invalid production options"));
    assert!(example.contains("write_summary_artifact_atomic"));
    assert!(example.contains("fs::create_dir_all(parent)"));
    assert!(example.contains("File::create(&tmp_path)"));
    assert!(example.contains("tmp_file.write_all"));
    assert!(example.contains("tmp_file.sync_all()"));
    assert!(example.contains("fs::rename(&tmp_path, summary_out)"));
    assert!(example.contains("fs::remove_file(&tmp_path)"));
}

#[cfg(unix)]
#[test]
fn direct_baseline_raft_benchmark_example_writes_summary_atomically() {
    let root = temp_dir("direct-summary-atomic");
    let fake_bin = make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);
    let summary_dir = root.join("artifacts");
    let summary_path = summary_dir.join("summary.json");

    let output = Command::new("cargo")
        .arg("run")
        .arg("--quiet")
        .arg("--example")
        .arg("baseline_raft_parity_benchmark")
        .env("BASELINE_RAFT_ROOT", &root)
        .env("BASELINE_RAFT_BENCHMARK_BIN", &fake_bin)
        .env(
            "RUSTRAFT_BENCHMARK_ITERATIONS",
            MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD.to_string(),
        )
        .env("RUSTRAFT_BENCHMARK_BATCH_SIZE", "2")
        .env("RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES", "4096")
        .env("RUSTRAFT_BENCHMARK_SUMMARY_OUT", &summary_path)
        .output()
        .expect("run direct benchmark example");

    assert_eq!(
        output.status.code(),
        Some(1),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("benchmark:report_environment_debug_assertions_enabled"));
    let summary = fs::read_to_string(&summary_path).expect("summary artifact");
    let summary_json: serde_json::Value = serde_json::from_str(&summary).expect("summary json");
    assert_eq!(
        summary_json["schema"],
        "rustraft.baseline_raft_benchmark_summary.v1"
    );
    let leftovers = fs::read_dir(&summary_dir)
        .expect("summary dir")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .starts_with(".summary.json.")
        })
        .collect::<Vec<_>>();
    assert!(
        leftovers.is_empty(),
        "temporary summary artifacts left behind: {:?}",
        leftovers
            .iter()
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>()
    );

    let _ = fs::remove_dir_all(root);
}

#[test]
fn readme_documents_release_scale_benchmark_contract() {
    let readme =
        fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
            .expect("read README");

    assert!(readme.contains("scripts/baseline_raft_vs_rustraft_benchmark.sh"));
    assert!(readme.contains("--release"));
    assert!(readme.contains("--node-count 5"));
    assert!(readme.contains("--iterations 128"));
    assert!(readme.contains("--batch-size 16"));
    assert!(readme.contains("--payload-size-bytes 4096"));
    assert!(readme.contains("--pass-tolerance-percent 10.0"));
    assert!(
        readme.contains("--summary-out target/baseline_raft-vs-rustraft-benchmark/summary.json")
    );
    assert!(readme.contains("at least 5 nodes"));
    assert!(readme.contains("at least 128 iterations per workload"));
    assert!(readme.contains("payloads must be at least 4096 bytes"));
    assert!(readme.contains("pass tolerance must be finite and no higher than 10%"));
    assert!(readme.contains("scripts/verify_baseline_raft_benchmark_artifacts.sh"));
}

#[test]
fn benchmark_artifact_verifier_script_defaults_to_release() {
    let verify_script = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("verify_baseline_raft_benchmark_artifacts.sh"),
    )
    .expect("read verifier script");

    assert!(verify_script.contains("cargo_profile=(--release)"));
    assert!(verify_script.contains("--release)"));
    assert!(verify_script.contains("--debug)"));
    assert!(verify_script.contains("cargo_profile=()"));
    assert!(verify_script.contains("The verifier runs in release mode by default"));
    assert!(verify_script.contains("\"${cargo_profile[@]}\""));
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_non_executable_harness_before_cargo_run() {
    let root = temp_dir("script-non-executable-harness");
    let baseline_raft_root = root.join("baseline_raft");
    let bin_dir = baseline_raft_root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let harness = bin_dir.join("baseline_raft_parity_benchmark");
    fs::write(&harness, "#!/usr/bin/env bash\necho should-not-run\n").expect("write harness");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(&baseline_raft_root)
        .arg("--baseline_raft-bin")
        .arg(&harness)
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:real_baseline_raft_harness_not_executable"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_invalid_dimensions_before_harness_lookup() {
    let root = temp_dir("script-invalid-dimensions");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--iterations")
        .arg("0")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_iterations_per_workload:0"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_tiny_iterations_before_harness_lookup() {
    let root = temp_dir("script-tiny-iterations");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--iterations")
        .arg("2")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:iterations_per_workload_below_production_min:2:128"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_below_scale_node_count_before_harness_lookup() {
    let root = temp_dir("script-below-scale-node-count");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--node-count")
        .arg("3")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:node_count_below_production_scale:3:5"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_unbatched_batch_size_before_harness_lookup() {
    let root = temp_dir("script-unbatched-batch-size");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--batch-size")
        .arg("1")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:batch_size_below_production_min:1:2"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_tiny_payload_before_harness_lookup() {
    let root = temp_dir("script-tiny-payload");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--payload-size-bytes")
        .arg("1024")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:payload_size_below_production_min:1024:4096"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_loose_tolerance_before_harness_lookup() {
    let root = temp_dir("script-loose-tolerance");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--pass-tolerance-percent")
        .arg("10.1")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:pass_tolerance_above_production_max:10.100:10.000"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_invalid_tolerance_before_harness_lookup() {
    let root = temp_dir("script-invalid-tolerance");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--pass-tolerance-percent")
        .arg("loose")
        .arg("--out")
        .arg(&report)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_pass_tolerance_above_production_max:loose"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_invalid_artifact_age_before_harness_lookup() {
    let root = temp_dir("script-invalid-artifact-age");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--out")
        .arg(&report)
        .env("RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS", "0")
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_max_artifact_age_seconds:0"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_artifact_path_collision_before_harness_lookup() {
    let root = temp_dir("script-artifact-path-collision");
    fs::create_dir_all(&root).expect("root");
    let artifact = root.join("report.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--out")
        .arg(&artifact)
        .arg("--summary-out")
        .arg(&artifact)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:artifact_path_collision:report_summary"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!artifact.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_empty_report_path_before_harness_lookup() {
    let root = temp_dir("script-empty-report-path");
    fs::create_dir_all(&root).expect("root");
    let summary = root.join("summary.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--out")
        .arg("")
        .arg("--summary-out")
        .arg(&summary)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_report_path:empty"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!summary.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_directory_report_path_before_harness_lookup() {
    let root = temp_dir("script-directory-report-path");
    fs::create_dir_all(&root).expect("root");
    let report_dir = root.join("report-dir");
    fs::create_dir_all(&report_dir).expect("report dir");
    let summary = root.join("summary.json");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--out")
        .arg(&report_dir)
        .arg("--summary-out")
        .arg(&summary)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_report_path:is_directory"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!summary.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_rejects_directory_summary_path_before_harness_lookup() {
    let root = temp_dir("script-directory-summary-path");
    fs::create_dir_all(&root).expect("root");
    let report = root.join("report.json");
    let summary_dir = root.join("summary-dir");
    fs::create_dir_all(&summary_dir).expect("summary dir");

    let benchmark_script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("baseline_raft_vs_rustraft_benchmark.sh");
    let output = Command::new("bash")
        .arg(benchmark_script)
        .arg("--rustraft-root")
        .arg(env!("CARGO_MANIFEST_DIR"))
        .arg("--baseline_raft-root")
        .arg(root.join("missing-baseline_raft"))
        .arg("--out")
        .arg(&report)
        .arg("--summary-out")
        .arg(&summary_dir)
        .output()
        .expect("run benchmark script");

    assert_eq!(
        output.status.code(),
        Some(2),
        "stdout={}\nstderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("benchmark:invalid_summary_path:is_directory"));
    assert!(!stderr.contains("benchmark:real_baseline_raft_missing"));
    assert!(!report.exists());
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_benchmark_script_does_not_treat_non_executable_native_bins_as_runnable() {
    let benchmark_script = fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("scripts")
            .join("baseline_raft_vs_rustraft_benchmark.sh"),
    )
    .expect("read benchmark script");

    assert!(
        benchmark_script.contains("BaselineRaft native capability: kvbench=${kvbench:-missing}")
    );
    assert!(benchmark_script.contains("export BASELINE_RAFT_KVSERVER_BIN=\"$kvserver_candidate\""));
    assert!(benchmark_script.contains("export BASELINE_RAFT_KVBENCH_BIN=\"$kvbench_candidate\""));
    assert_eq!(
        benchmark_script.matches("[[ -x \"$candidate\" ]]").count(),
        4,
        "BaselineRaft benchmark, native kvserver, native kvbench, and native capability discovery should all require executable candidates"
    );
    assert!(!benchmark_script.contains("[[ -x \"$candidate\" || -f \"$candidate\" ]]"));
}

#[cfg(unix)]
fn make_fake_baseline_raft_harness(root: &std::path::Path) -> PathBuf {
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let bin = bin_dir.join("baseline_raft_parity_benchmark");
    write_executable(
        &bin,
        r#"#!/usr/bin/env bash
set -euo pipefail
workload="single_key_writes"
node_count=5
iterations=4
batch_size=1
payload_size_bytes=0
wal_dir=""
snapshot_dir=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload) workload="$2"; shift 2 ;;
    --node-count) node_count="$2"; shift 2 ;;
    --iterations) iterations="$2"; shift 2 ;;
    --batch-size) batch_size="$2"; shift 2 ;;
    --payload-size-bytes) payload_size_bytes="$2"; shift 2 ;;
    --wal-dir) wal_dir="$2"; shift 2 ;;
    --snapshot-dir) snapshot_dir="$2"; shift 2 ;;
    --baseline_raft-root) shift 2 ;;
    *) shift ;;
  esac
done
operation_count="$iterations"
operations_per_timed_iteration=1
case "$workload" in
  batched_writes|replication_batching)
    operation_count=$((iterations * batch_size))
    operations_per_timed_iteration="$batch_size"
    ;;
esac
total_duration_micros=$((operation_count * 1000000))
{
  echo "workload=$workload"
  echo "node_count=$node_count"
  echo "iterations=$iterations"
  echo "batch_size=$batch_size"
  echo "payload_size_bytes=$payload_size_bytes"
  echo "wal_dir_exists=$([[ -d "$wal_dir" ]] && echo yes || echo no)"
  echo "snapshot_dir_exists=$([[ -d "$snapshot_dir" ]] && echo yes || echo no)"
} > "$(dirname "$0")/last_args.txt"
cat <<JSON
{
  "workload": "$workload",
  "engine": "baseline_raft",
  "engine_source": "real_baseline_raft",
  "implementation": "baseline_raft",
  "binary_path": null,
  "git_revision": "$(git -C "$(dirname "$0")/.." rev-parse HEAD)",
  "build_profile": "release",
  "harness_kind": "full_baseline_raft_harness",
  "node_count": $node_count,
  "iterations_per_workload": $iterations,
  "batch_size": $batch_size,
  "payload_size_bytes": $payload_size_bytes,
  "timed_iteration_count": $iterations,
  "operations_per_timed_iteration": $operations_per_timed_iteration,
  "total_duration_micros": $total_duration_micros,
  "operation_count": $operation_count,
  "p50_latency_micros": 1000000,
  "p99_latency_micros": 1000000,
  "throughput_ops_per_sec": 1.0,
  "correctness_passed": true
}
JSON
"#,
    );
    bin
}

#[cfg(unix)]
fn write_executable(path: &std::path::Path, content: &str) {
    use std::os::unix::fs::PermissionsExt;

    fs::write(path, content).expect("write executable");
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

#[cfg(unix)]
fn make_fake_git_checkout(root: &std::path::Path) {
    for args in [
        vec!["init"],
        vec!["config", "user.email", "rustraft-test@example.invalid"],
        vec!["config", "user.name", "RustRaft Test"],
        vec!["add", "."],
        vec!["commit", "-m", "fake baseline_raft harness"],
    ] {
        let output = Command::new("git")
            .arg("-C")
            .arg(root)
            .args(args)
            .output()
            .expect("run git");
        assert!(
            output.status.success(),
            "git failed: stdout={}\nstderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_uses_external_harness_and_production_sources() {
    let root = temp_dir("root");
    let fake_bin = make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);
    assert_eq!(
        matrixraft_find_baseline_raft_harness(&root).expect("find"),
        fake_bin
    );

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    let debug_error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("debug-built test artifact is not production release parity");
    assert!(debug_error.contains("benchmark:report_environment_debug_assertions_enabled"));
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    matrixraft_assert_production_baseline_raft_parity(&report).expect("production parity");
    assert!(report.generated_at_unix_ms > 0);

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    assert_eq!(summary.generated_at_unix_ms, report.generated_at_unix_ms);
    matrixraft_assert_production_baseline_raft_summary(&summary).expect("production summary");

    let evidence = matrixraft_baseline_raft_benchmark_evidence(&report);
    assert!(evidence.real_baseline_raft);
    assert!(evidence.matrixraft_runtime);
    assert!(evidence.baseline_raft_reference);
    assert!(evidence.matrixraft_rust_candidate);
    assert!(evidence.correctness_passed);
    assert!(evidence.performance_within_threshold);
    assert!(evidence.blockers.is_empty());
    assert!(report.comparisons.iter().all(|comparison| {
        comparison.baseline_raft.engine_source == RustRaftBenchmarkEngineSource::RealBaselineRaft
            && comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::RustRaftRuntime
    }));
    let captured_args = fs::read_to_string(root.join("bin/last_args.txt")).expect("captured args");
    assert!(captured_args.contains("node_count=5"));
    assert!(captured_args.contains(&format!(
        "iterations={MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD}"
    )));
    assert!(captured_args.contains("batch_size=2"));
    assert!(captured_args.contains("payload_size_bytes=4096"));
    assert!(captured_args.contains("wal_dir_exists=yes"));
    assert!(captured_args.contains("snapshot_dir_exists=yes"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_stale_report_and_summary_timestamps() {
    let root = temp_dir("stale-report");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 2,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.generated_at_unix_ms = 1;

    let error =
        matrixraft_assert_production_baseline_raft_parity(&report).expect_err("stale report");
    assert!(error.contains("benchmark:report_generated_at_stale"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error =
        matrixraft_assert_production_baseline_raft_summary(&summary).expect_err("stale summary");
    assert!(summary_error.contains("benchmark:summary_generated_at_stale"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_reports_use_unique_process_scoped_run_ids() {
    let root = temp_dir("unique-run-id");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let first =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    let second =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    assert_ne!(first.benchmark_run_id, second.benchmark_run_id);
    for report in [&first, &second] {
        assert!(report
            .benchmark_run_id
            .starts_with("rustraft-baseline-raft-parity-"));
        assert!(
            report
                .benchmark_run_id
                .contains(&format!("-pid-{}-seq-", std::process::id())),
            "{}",
            report.benchmark_run_id
        );
        for comparison in &report.comparisons {
            assert_eq!(
                comparison.baseline_raft.benchmark_run_id,
                report.benchmark_run_id
            );
            assert_eq!(
                comparison.rustraft.benchmark_run_id,
                report.benchmark_run_id
            );
        }
    }

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_non_finite_pass_tolerance() {
    let root = temp_dir("nan-tolerance");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 2,
        batch_size: 2,
        payload_size_bytes: 4096,
        pass_tolerance_percent: f64::NAN,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error =
        matrixraft_assert_production_baseline_raft_parity(&report).expect_err("invalid tolerance");
    assert!(error.contains("benchmark:invalid_pass_tolerance_percent:NaN"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_loose_pass_tolerance() {
    let root = temp_dir("loose-tolerance");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        pass_tolerance_percent: MATRIXRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT + 0.1,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error =
        matrixraft_assert_production_baseline_raft_parity(&report).expect_err("loose tolerance");
    assert!(error.contains("benchmark:invalid_pass_tolerance_percent:10.100"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error =
        matrixraft_assert_production_baseline_raft_summary(&summary).expect_err("loose tolerance");
    assert!(summary_error.contains("benchmark:invalid_pass_tolerance_percent:10.100"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_tiny_iteration_count() {
    let root = temp_dir("tiny-iteration-count");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: 2,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error =
        matrixraft_assert_production_baseline_raft_parity(&report).expect_err("tiny iterations");
    assert!(error.contains("benchmark:iterations_per_workload_below_production_min:2:128"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error =
        matrixraft_assert_production_baseline_raft_summary(&summary).expect_err("tiny iterations");
    assert!(summary_error.contains("benchmark:iterations_per_workload_below_production_min:2:128"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_unbatched_batch_size() {
    let root = temp_dir("unbatched-batch-size");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 1,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("unbatched batch size must not satisfy production parity");
    assert!(error.contains("benchmark:batch_size_below_production_min:1:2"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject unbatched batch size");
    assert!(summary_error.contains("benchmark:batch_size_below_production_min:1:2"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_tiny_payload_size() {
    let root = temp_dir("tiny-payload-size");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES - 1,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("tiny payload size must not satisfy production parity");
    assert!(error.contains("benchmark:payload_size_below_production_min:4095:4096"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject tiny payload size");
    assert!(summary_error.contains("benchmark:payload_size_below_production_min:4095:4096"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_below_scale_node_count() {
    let root = temp_dir("below-scale-node-count");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        node_count: 3,
        iterations_per_workload: 2,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);

    let error =
        matrixraft_assert_production_baseline_raft_parity(&report).expect_err("below scale");
    assert!(error.contains("benchmark:node_count_below_production_scale:3:5"));

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error =
        matrixraft_assert_production_baseline_raft_summary(&summary).expect_err("below scale");
    assert!(summary_error.contains("benchmark:node_count_below_production_scale:3:5"));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_non_runtime_rustraft_harness() {
    let root = temp_dir("rustraft-harness-kind");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.harness_kind = RustRaftBenchmarkHarnessKind::Model;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("non-runtime rustraft harness must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_runtime_harness_missing:model"),
        "{error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_sample_engine_mismatch() {
    let root = temp_dir("sample-engine-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.engine = RustRaftBenchmarkEngine::BaselineRaft;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("sample engine mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_sample_engine_mismatch"),
        "{error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_sample_engine_source_mismatch() {
    let root = temp_dir("sample-source-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.engine_source = RustRaftBenchmarkEngineSource::Model;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("sample engine source mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_sample_engine_source_mismatch"),
        "{error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_nonexistent_binary_provenance() {
    let root = temp_dir("binary-provenance");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.binary_path = Some(
        root.join("missing-rustraft-benchmark")
            .display()
            .to_string(),
    );

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("nonexistent binary provenance must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_provenance_binary_path_not_file"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject nonexistent binary provenance");
    assert!(
        summary_error.contains("benchmark:summary_rustraft_provenance_binary_path_not_file"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_non_executable_binary_provenance() {
    let root = temp_dir("binary-provenance-non-executable");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);
    let non_executable = root.join("rustraft-benchmark-not-executable");
    fs::write(&non_executable, b"not executable").expect("write non-executable binary fixture");

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.binary_path = Some(non_executable.display().to_string());

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("non-executable binary provenance must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_provenance_binary_path_not_executable"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject non-executable binary provenance");
    assert!(
        summary_error.contains("benchmark:summary_rustraft_provenance_binary_path_not_executable"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_relative_binary_provenance() {
    let root = temp_dir("relative-binary-provenance");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);
    let relative_binary = std::path::PathBuf::from(format!(
        "target/rustraft-relative-benchmark-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos()
    ));
    if let Some(parent) = relative_binary.parent() {
        fs::create_dir_all(parent).expect("relative binary parent");
    }
    write_executable(&relative_binary, "#!/usr/bin/env bash\nexit 0\n");

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.binary_path = Some(relative_binary.display().to_string());

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("relative binary provenance must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_provenance_binary_path_not_absolute"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject relative binary provenance");
    assert!(
        summary_error.contains("benchmark:summary_rustraft_provenance_binary_path_not_absolute"),
        "{summary_error}"
    );

    let _ = fs::remove_file(relative_binary);
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_binary_path_collision() {
    let root = temp_dir("binary-path-collision");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    let reference_binary = report.comparisons[0]
        .baseline_raft
        .binary_path
        .clone()
        .expect("reference binary path");
    report.comparisons[0].rustraft.binary_path = Some(reference_binary);

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("binary path collision must not satisfy production parity");
    assert!(error.contains("benchmark:binary_path_collision"), "{error}");

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject binary path collision");
    assert!(
        summary_error.contains("benchmark:summary_binary_path_collision:single_key_writes"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_evidence_classifies_operation_count_pair_mismatch_as_correctness() {
    let root = temp_dir("operation-count-pair-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.comparisons[0]
        .blockers
        .push("operation_count_mismatch_baseline_raft_128_rustraft_64".to_string());

    let evidence = matrixraft_baseline_raft_benchmark_evidence(&report);
    assert!(
        evidence.correctness_blockers.iter().any(|blocker| blocker
            .contains("benchmark:operation_count_mismatch_baseline_raft_128_rustraft_64")),
        "{:#?}",
        evidence.correctness_blockers
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn benchmark_evidence_classifies_inconsistent_comparison_status_as_correctness() {
    let root = temp_dir("inconsistent-comparison-status");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.comparisons[0].passed = true;
    report.comparisons[0]
        .blockers
        .push("stale_imported_blocker".to_string());

    let evidence = matrixraft_baseline_raft_benchmark_evidence(&report);
    assert!(
        evidence
            .correctness_blockers
            .iter()
            .any(|blocker| blocker.contains("benchmark:comparison_passed_with_blockers")),
        "{:#?}",
        evidence.correctness_blockers
    );

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("passed row with blockers must not satisfy production parity");
    assert!(
        error.contains("benchmark:comparison_passed_with_blockers"),
        "{error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_summary_rejects_inconsistent_workload_status() {
    let root = temp_dir("summary-inconsistent-workload-status");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    let mut summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    summary.workloads[0].passed = true;
    summary.workloads[0]
        .blockers
        .push("stale_summary_blocker".to_string());

    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary workload status must be internally consistent");
    assert!(
        summary_error.contains("benchmark:summary_workload_passed_with_blockers"),
        "{summary_error}"
    );

    summary.workloads[0].blockers.clear();
    summary.workloads[0].p99_ratio = 2.0;
    let regression_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary workload must not pass with a measured regression");
    assert!(
        regression_error.contains("benchmark:summary_workload_passed_despite_regression"),
        "{regression_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_mismatched_timed_iteration_count() {
    let root = temp_dir("timed-iteration-count-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.timed_iteration_count = 1;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("timed iteration mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_sample_timed_iteration_count_mismatch:1:128"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject timed iteration mismatch");
    assert!(
        summary_error.contains(
            "benchmark:summary_rustraft_timed_iteration_count_mismatch:single_key_writes:1:128"
        ),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_mismatched_operations_per_timed_iteration() {
    let root = temp_dir("operations-per-timed-iteration-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    let batched = report
        .comparisons
        .iter_mut()
        .find(|comparison| comparison.workload == RustRaftBenchmarkWorkload::BatchedWrites)
        .expect("batched writes comparison");
    batched.rustraft.operations_per_timed_iteration = 1;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("operation/sample mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_sample_operations_per_timed_iteration_mismatch:1:2"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject operation/sample mismatch");
    assert!(
        summary_error.contains(
            "benchmark:summary_rustraft_operations_per_timed_iteration_mismatch:batched_writes:1:2"
        ),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_required_workload_manifest_mismatch() {
    let root = temp_dir("required-workload-manifest-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.required_workloads.pop();

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("required workload manifest mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:report_required_workloads_mismatch"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject required workload manifest mismatch");
    assert!(
        summary_error.contains("benchmark:summary_required_workloads_mismatch"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_required_workload_order_mismatch() {
    let root = temp_dir("required-workload-order-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons.swap(0, 1);

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("workload row order mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:report_workload_order_mismatch"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject workload row order mismatch");
    assert!(
        summary_error.contains("benchmark:summary_workload_order_mismatch"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_nonfinite_comparison_ratios() {
    let root = temp_dir("nonfinite-comparison-ratio");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].p99_ratio = f64::NAN;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("nonfinite comparison ratio must not satisfy production parity");
    assert!(
        error.contains("benchmark:comparison_p99_ratio_not_finite"),
        "{error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_summary_rejects_nonfinite_workload_ratios() {
    let root = temp_dir("summary-nonfinite-workload-ratio");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    let mut summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    summary.workloads[0].p99_ratio = f64::NAN;

    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("nonfinite summary workload ratio must not satisfy production parity");
    assert!(
        summary_error.contains("benchmark:summary_workload_p99_ratio_not_finite"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_sample_run_id_pair_mismatch() {
    let root = temp_dir("sample-run-id-pair-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.benchmark_run_id = "rustraft-other-run".to_string();

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("sample run id pair mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:report_sample_run_id_pair_mismatch"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject sample run id pair mismatch");
    assert!(
        summary_error.contains("benchmark:summary_sample_run_id_pair_mismatch"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_throughput_duration_mismatch() {
    let root = temp_dir("throughput-duration-mismatch");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].baseline_raft.total_duration_micros = 1;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("throughput/duration mismatch must not satisfy production parity");
    assert!(
        error.contains("benchmark:baseline_raft_sample_throughput_duration_mismatch"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject throughput/duration mismatch");
    assert!(
        summary_error.contains(
            "benchmark:summary_baseline_raft_throughput_duration_mismatch:single_key_writes"
        ),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_latency_exceeding_total_duration() {
    let root = temp_dir("latency-exceeds-total-duration");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    let total_duration = report.comparisons[0].rustraft.total_duration_micros;
    report.comparisons[0].rustraft.p99_latency_micros = total_duration + 1;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("latency over total duration must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_sample_p99_exceeds_total_duration"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject latency over total duration");
    assert!(
        summary_error
            .contains("benchmark:summary_rustraft_p99_exceeds_total_duration:single_key_writes"),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_reports_missing_regression_blocker_from_ratios() {
    let root = temp_dir("missing-regression-blocker");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();

    let comparison = &mut report.comparisons[0];
    comparison.rustraft.p99_latency_micros = comparison
        .baseline_raft
        .p99_latency_micros
        .saturating_mul(2);
    comparison.p99_ratio = comparison.rustraft.p99_latency_micros as f64
        / comparison.baseline_raft.p99_latency_micros as f64;
    comparison.passed = false;
    comparison.blockers.clear();
    report.passed = false;

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("missing regression blocker must not satisfy production parity");
    assert!(
        error.contains("benchmark:comparison_missing_p99_regression_blocker"),
        "{error}"
    );

    let evidence = matrixraft_baseline_raft_benchmark_evidence(&report);
    assert!(
        evidence
            .performance_blockers
            .iter()
            .any(|blocker| blocker.contains("benchmark:comparison_missing_p99_regression_blocker")),
        "{:#?}",
        evidence.performance_blockers
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn production_benchmark_rejects_invalid_git_revision_provenance() {
    let root = temp_dir("git-revision-provenance");
    make_fake_baseline_raft_harness(&root);
    make_fake_git_checkout(&root);

    let options = RustRaftBenchmarkOptions {
        iterations_per_workload: MATRIXRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD,
        batch_size: 2,
        payload_size_bytes: 4096,
        ..Default::default()
    };
    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "release").expect("runner");
    let mut rustraft = RustRaftRuntimeBenchmarkRunner::new("release");
    let mut report =
        matrixraft_run_baseline_raft_parity_benchmark(&mut baseline_raft, &mut rustraft, &options);
    report.environment_fingerprint =
        "os=linux;arch=x86_64;target=x86_64-unknown-linux-gnu;debug_assertions=false".to_string();
    report.comparisons[0].rustraft.git_revision = Some("rustraft-test-rev".to_string());

    let error = matrixraft_assert_production_baseline_raft_parity(&report)
        .expect_err("invalid git revision provenance must not satisfy production parity");
    assert!(
        error.contains("benchmark:rustraft_provenance_git_revision_invalid:rustraft-test-rev"),
        "{error}"
    );

    let summary = matrixraft_baseline_raft_benchmark_failure_summary(&report);
    let summary_error = matrixraft_assert_production_baseline_raft_summary(&summary)
        .expect_err("summary must also reject invalid git revision provenance");
    assert!(
        summary_error.contains(
            "benchmark:summary_rustraft_provenance_git_revision_invalid:single_key_writes:rustraft-test-rev",
        ),
        "{summary_error}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_reports_harness_failure_as_workload_blocker() {
    let root = temp_dir("harness-failure");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    write_executable(
        &bin_dir.join("baseline_raft_parity_benchmark"),
        r#"#!/usr/bin/env bash
echo "simulated BaselineRaft harness failure" >&2
exit 17
"#,
    );

    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "fake-failure").expect("runner");
    let sample = baseline_raft.run_workload(
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        &RustRaftBenchmarkOptions {
            iterations_per_workload: 1,
            ..Default::default()
        },
    );

    assert_eq!(
        sample.engine_source,
        RustRaftBenchmarkEngineSource::RealBaselineRaft
    );
    assert!(!sample.correctness_passed);
    assert_eq!(sample.operation_count, 1);
    assert!(sample.blockers.iter().any(|blocker| blocker
        .contains("benchmark:real_baseline_raft_harness_failed:single_key_writes:exit_17")));
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_rejects_non_executable_harness_before_spawn() {
    let root = temp_dir("non-executable-runner");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let harness = bin_dir.join("baseline_raft_parity_benchmark");
    fs::write(&harness, "#!/usr/bin/env bash\necho should-not-run\n").expect("write harness");

    let error =
        RustRaftExternalBaselineRaftRunner::new(&harness, Some(&root), "fake-non-executable")
            .expect_err("non-executable harness must be rejected");
    assert!(error.contains("benchmark:real_baseline_raft_harness_not_executable"));
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_harness_discovery_rejects_non_executable_candidate() {
    let root = temp_dir("non-executable-discovery");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let harness = bin_dir.join("baseline_raft_parity_benchmark");
    fs::write(&harness, "#!/usr/bin/env bash\necho should-not-run\n").expect("write harness");

    let error = matrixraft_find_baseline_raft_harness(&root).expect_err("non-executable harness");
    assert!(error.contains("benchmark:real_baseline_raft_harness_not_executable"));
    let error = matrixraft_find_or_build_baseline_raft_harness(&root, "debug")
        .expect_err("non-executable build");
    assert!(error.contains("benchmark:real_baseline_raft_harness_not_executable"));
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_reports_invalid_json_as_workload_blocker() {
    let root = temp_dir("invalid-json");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    write_executable(
        &bin_dir.join("baseline_raft_parity_benchmark"),
        r#"#!/usr/bin/env bash
echo "not-json"
"#,
    );

    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "fake-invalid-json").expect("runner");
    let sample = baseline_raft.run_workload(
        RustRaftBenchmarkWorkload::SnapshotStreaming,
        &RustRaftBenchmarkOptions {
            iterations_per_workload: 1,
            ..Default::default()
        },
    );

    assert_eq!(
        sample.engine_source,
        RustRaftBenchmarkEngineSource::RealBaselineRaft
    );
    assert!(!sample.correctness_passed);
    assert_eq!(sample.operation_count, 1);
    assert!(sample.blockers.iter().any(|blocker| blocker
        .contains("benchmark:real_baseline_raft_harness_invalid_json:snapshot_streaming")));
    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_stamps_and_validates_runner_owned_provenance() {
    let root = temp_dir("bad-provenance");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    let harness = bin_dir.join("baseline_raft_parity_benchmark");
    write_executable(
        &harness,
        r#"#!/usr/bin/env bash
cat <<JSON
{
  "workload": "single_key_writes",
  "engine": "baseline_raft",
  "engine_source": "real_baseline_raft",
  "implementation": "baseline_raft",
  "binary_path": "/tmp/not-the-running-baseline_raft-binary",
  "git_revision": "fake-baseline_raft-revision",
  "build_profile": "release",
  "harness_kind": "full_baseline_raft_harness",
  "node_count": 3,
  "iterations_per_workload": 1,
  "batch_size": 16,
  "payload_size_bytes": 1024,
  "operation_count": 1,
  "p50_latency_micros": 1000000,
  "p99_latency_micros": 1000000,
  "throughput_ops_per_sec": 1.0,
  "correctness_passed": true
}
JSON
"#,
    );
    make_fake_git_checkout(&root);

    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "debug").expect("runner");
    let sample = baseline_raft.run_workload(
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        &RustRaftBenchmarkOptions {
            iterations_per_workload: 1,
            ..Default::default()
        },
    );

    assert_eq!(sample.binary_path, Some(harness.display().to_string()));
    assert_eq!(sample.build_profile, "debug");
    assert!(!sample.correctness_passed);
    assert!(sample.blockers.iter().any(|blocker| {
        blocker
            .contains("benchmark:real_baseline_raft_harness_binary_path_mismatch:single_key_writes")
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker.contains(
            "benchmark:real_baseline_raft_harness_git_revision_mismatch:single_key_writes",
        )
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker.contains(
            "benchmark:real_baseline_raft_harness_build_profile_mismatch:single_key_writes:release:debug",
        )
    }));

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_rejects_invalid_latency_order() {
    let root = temp_dir("bad-latency-order");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    write_executable(
        &bin_dir.join("baseline_raft_parity_benchmark"),
        r#"#!/usr/bin/env bash
cat <<JSON
{
  "workload": "single_key_writes",
  "engine": "baseline_raft",
  "engine_source": "real_baseline_raft",
  "implementation": "baseline_raft",
  "binary_path": null,
  "git_revision": "fake-baseline_raft-revision",
  "build_profile": "fake-test",
  "harness_kind": "full_baseline_raft_harness",
  "node_count": 5,
  "iterations_per_workload": 1,
  "batch_size": 16,
  "payload_size_bytes": 1024,
  "operation_count": 1,
  "p50_latency_micros": 300,
  "p99_latency_micros": 200,
  "throughput_ops_per_sec": 1000.0,
  "correctness_passed": true
}
JSON
"#,
    );

    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "fake-test").expect("runner");
    let sample = baseline_raft.run_workload(
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        &RustRaftBenchmarkOptions {
            iterations_per_workload: 1,
            batch_size: 16,
            ..Default::default()
        },
    );

    assert!(!sample.correctness_passed);
    assert!(
        sample.blockers.iter().any(|blocker| {
            blocker.contains(
            "benchmark:real_baseline_raft_harness_latency_order_invalid:single_key_writes:300:200",
        )
        }),
        "{sample:#?}"
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn real_baseline_raft_runner_rejects_malformed_success_sample_shape() {
    let root = temp_dir("bad-success-shape");
    let bin_dir = root.join("bin");
    fs::create_dir_all(&bin_dir).expect("bin dir");
    write_executable(
        &bin_dir.join("baseline_raft_parity_benchmark"),
        r#"#!/usr/bin/env bash
cat <<JSON
{
  "workload": "single_key_writes",
  "engine": "rust_raft",
  "engine_source": "model",
  "implementation": "model",
  "binary_path": null,
  "git_revision": "fake-baseline_raft-revision",
  "build_profile": "fake-test",
  "harness_kind": "full_baseline_raft_harness",
  "node_count": 1,
  "iterations_per_workload": 999,
  "batch_size": 999,
  "payload_size_bytes": 0,
  "operation_count": 999,
  "p50_latency_micros": 0,
  "p99_latency_micros": 0,
  "throughput_ops_per_sec": 0.0,
  "correctness_passed": true
}
JSON
"#,
    );

    let mut baseline_raft =
        RustRaftExternalBaselineRaftRunner::from_root(&root, "fake-test").expect("runner");
    let sample = baseline_raft.run_workload(
        RustRaftBenchmarkWorkload::BatchedWrites,
        &RustRaftBenchmarkOptions {
            iterations_per_workload: 2,
            batch_size: 2,
            ..Default::default()
        },
    );

    assert!(!sample.correctness_passed);
    assert!(sample.blockers.iter().any(|blocker| blocker.contains(
        "benchmark:real_baseline_raft_harness_workload_mismatch:batched_writes:single_key_writes"
    )));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker.contains("benchmark:real_baseline_raft_harness_engine_mismatch:batched_writes")
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker
            .contains("benchmark:real_baseline_raft_harness_engine_source_mismatch:batched_writes")
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker.contains(
            "benchmark:real_baseline_raft_harness_implementation_mismatch:batched_writes:model:baseline_raft",
        )
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker
            .contains("benchmark:real_baseline_raft_harness_node_count_mismatch:batched_writes:1:5")
    }));
    assert!(sample.blockers.iter().any(|blocker| {
        blocker.contains(
            "benchmark:real_baseline_raft_harness_operation_count_mismatch:batched_writes:999:4",
        )
    }));
    assert!(sample
        .blockers
        .iter()
        .any(|blocker| blocker.contains("benchmark:real_baseline_raft_harness_p50_latency_zero")));
    assert!(
        sample
            .blockers
            .iter()
            .any(|blocker| blocker
                .contains("benchmark:real_baseline_raft_harness_throughput_invalid"))
    );

    let _ = fs::remove_dir_all(root);
}

#[cfg(unix)]
#[test]
fn baseline_raft_runner_builds_harness_from_checkout_hook_before_failing_closed() {
    use std::os::unix::fs::PermissionsExt;

    let root = temp_dir("build-hook");
    let scripts = root.join("scripts");
    fs::create_dir_all(&scripts).expect("scripts");
    let build_script = scripts.join("build_baseline_raft_parity_benchmark.sh");
    fs::write(
        &build_script,
        r#"#!/usr/bin/env bash
set -euo pipefail
mkdir -p bin
cat > bin/baseline_raft_parity_benchmark <<'HARNESS'
#!/usr/bin/env bash
cat <<JSON
{
  "workload": "single_key_writes",
  "engine": "baseline_raft",
  "engine_source": "real_baseline_raft",
  "implementation": "baseline_raft",
  "binary_path": null,
  "git_revision": null,
  "build_profile": "fake-build-hook",
  "harness_kind": "full_baseline_raft_harness",
  "node_count": 3,
  "iterations_per_workload": 1,
  "batch_size": 16,
  "payload_size_bytes": 1024,
  "operation_count": 1,
  "p50_latency_micros": 1000000,
  "p99_latency_micros": 1000000,
  "throughput_ops_per_sec": 1.0,
  "correctness_passed": true
}
JSON
HARNESS
chmod +x bin/baseline_raft_parity_benchmark
"#,
    )
    .expect("build script");
    let mut perms = fs::metadata(&build_script).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&build_script, perms).expect("chmod");

    let built =
        matrixraft_find_or_build_baseline_raft_harness(&root, "debug").expect("build fake harness");

    assert_eq!(built, root.join("bin/baseline_raft_parity_benchmark"));
    assert!(built.is_file());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn missing_baseline_raft_harness_fails_closed_with_real_baseline_raft_blocker() {
    let root = temp_dir("missing");
    fs::create_dir_all(&root).expect("root");
    let err = matrixraft_find_baseline_raft_harness(&root).expect_err("missing harness");
    assert!(err.contains("benchmark:real_baseline_raft_missing"));
    let err = matrixraft_find_or_build_baseline_raft_harness(&root, "debug")
        .expect_err("missing build target");
    assert!(err.contains("benchmark:real_baseline_raft_missing"));
    let _ = fs::remove_dir_all(root);
}

#[test]
fn native_baseline_raft_kvbench_is_reported_as_partial_not_production_parity() {
    let root = temp_dir("native-kvbench");
    fs::create_dir_all(root.join("example/kv")).expect("kv dir");
    fs::create_dir_all(root.join("script")).expect("script dir");
    fs::write(
        root.join("example/kv/kv_benchmark.cc"),
        "int main() { return 0; }",
    )
    .expect("kv source");
    fs::write(
        root.join("example/kv/kv_server.cc"),
        "int main() { return 0; }",
    )
    .expect("kv server source");
    fs::write(
        root.join("example/kv/CMakeLists.txt"),
        "add_executable(kvserver kv_server.cc)\nadd_executable(kvbench kv_benchmark.cc)",
    )
    .expect("cmake source");
    fs::write(root.join("script/bench.sh"), "#!/usr/bin/env bash\n").expect("bench script");

    let capability = matrixraft_probe_baseline_raft_native_benchmark(&root);

    assert_eq!(
        capability.kvbench_source_path,
        Some(
            root.join("example/kv/kv_benchmark.cc")
                .display()
                .to_string()
        )
    );
    assert_eq!(
        capability.kvserver_source_path,
        Some(root.join("example/kv/kv_server.cc").display().to_string())
    );
    assert_eq!(
        capability.bench_script_path,
        Some(root.join("script/bench.sh").display().to_string())
    );
    assert!(capability.cmake_kvserver_target_present);
    assert!(capability.cmake_kvbench_target_present);
    assert_eq!(capability.kvserver_binary_path, None);
    assert_eq!(capability.kvbench_binary_path, None);
    assert!(capability.supported_workloads.is_empty());
    assert!(capability
        .missing_required_workloads
        .contains(&"single_key_writes".to_string()));
    assert!(capability
        .missing_required_workloads
        .contains(&"leader_transfer_under_load".to_string()));
    assert!(capability
        .blockers
        .contains(&"benchmark:baseline_raft_kvserver_binary_missing".to_string()));
    assert!(capability
        .blockers
        .contains(&"benchmark:baseline_raft_kvbench_binary_missing".to_string()));
    assert!(capability
        .blockers
        .contains(&"benchmark:baseline_raft_native_kvbench_partial".to_string()));

    let err = matrixraft_find_or_build_baseline_raft_harness(&root, "debug")
        .expect_err("partial kvbench");
    assert!(err.contains("benchmark:real_baseline_raft_missing"));

    let _ = fs::remove_dir_all(root);
}

#[test]
fn native_baseline_raft_probe_reports_runnable_partial_workloads_when_kv_binaries_exist() {
    let root = temp_dir("native-kvbench-runnable");
    fs::create_dir_all(root.join("build/example/kv")).expect("build kv dir");
    fs::write(
        root.join("build/example/kv/kvserver"),
        "#!/usr/bin/env bash\n",
    )
    .expect("server");
    fs::write(
        root.join("build/example/kv/kvbench"),
        "#!/usr/bin/env bash\n",
    )
    .expect("bench");

    let capability = matrixraft_probe_baseline_raft_native_benchmark(&root);

    assert_eq!(
        capability.kvserver_binary_path,
        Some(root.join("build/example/kv/kvserver").display().to_string())
    );
    assert_eq!(
        capability.kvbench_binary_path,
        Some(root.join("build/example/kv/kvbench").display().to_string())
    );
    assert_eq!(
        capability.supported_workloads,
        vec![
            "single_key_writes".to_string(),
            "batched_writes".to_string(),
            "replication_batching".to_string(),
            "read_index_reads".to_string(),
            "lease_reads".to_string(),
        ]
    );
    assert!(capability
        .missing_required_workloads
        .contains(&"wal_fsync".to_string()));
    assert!(capability
        .missing_required_workloads
        .contains(&"snapshot_streaming".to_string()));
    assert!(!capability
        .blockers
        .contains(&"benchmark:baseline_raft_kvserver_binary_missing".to_string()));
    assert!(!capability
        .blockers
        .contains(&"benchmark:baseline_raft_kvbench_binary_missing".to_string()));
    assert!(capability
        .blockers
        .contains(&"benchmark:baseline_raft_native_kvbench_partial".to_string()));

    let _ = fs::remove_dir_all(root);
}
