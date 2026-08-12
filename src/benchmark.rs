// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{self, Command};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{
    rustraft_production_readiness_report, PersistentRaftWal, PersistentRaftWalOptions, RaftCluster,
    RaftConfig, RaftError, RustRaftApplySnapshotFence, RustRaftHardState, RustRaftLogEntry,
    RustRaftLogId, RustRaftMembership, RustRaftPeer, RustRaftProductionReadinessInput,
    RustRaftProductionReadinessReport, RustRaftReplicaRole, RustRaftSnapshotMeta,
    RustRaftWalRecord,
};

const RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_MS: u64 = 24 * 60 * 60 * 1000;
const RUSTRAFT_BENCHMARK_MAX_FUTURE_SKEW_MS: u64 = 60 * 1000;
pub const RUSTRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT: usize = 5;
pub const RUSTRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD: usize = 128;
pub const RUSTRAFT_BENCHMARK_MIN_PRODUCTION_BATCH_SIZE: usize = 2;
pub const RUSTRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES: usize = 4096;
pub const RUSTRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT: f64 = 10.0;
pub const RUSTRAFT_BENCHMARK_REPORT_SCHEMA: &str = "rustraft.reference_raft_benchmark_report.v1";
pub const RUSTRAFT_BENCHMARK_SUMMARY_SCHEMA: &str = "rustraft.reference_raft_benchmark_summary.v1";

static RUSTRAFT_BENCHMARK_RUN_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftBenchmarkEvidence {
    pub real_reference_raft: bool,
    pub rustraft_runtime: bool,
    #[serde(default)]
    pub reference_raft_cpp_reference: bool,
    #[serde(default)]
    pub rustraft_rust_candidate: bool,
    pub correctness_passed: bool,
    pub performance_within_threshold: bool,
    pub workloads: Vec<String>,
    pub blockers: Vec<String>,
    #[serde(default)]
    pub missing_reference_raft_binaries: Vec<String>,
    #[serde(default)]
    pub unsupported_workloads: Vec<String>,
    #[serde(default)]
    pub correctness_blockers: Vec<String>,
    #[serde(default)]
    pub performance_blockers: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkEngine {
    ReferenceRaft,
    RustRaft,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkEngineSource {
    RealReferenceRaft,
    RustRaftRuntime,
    Model,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum RustRaftBenchmarkImplementation {
    #[serde(rename = "reference_raft_cpp")]
    ReferenceRaftCpp,
    #[serde(rename = "rustraft_rust")]
    RustRaftRust,
    Model,
    #[default]
    Unknown,
}

impl RustRaftBenchmarkImplementation {
    pub fn id(self) -> &'static str {
        match self {
            Self::ReferenceRaftCpp => "reference_raft_cpp",
            Self::RustRaftRust => "rustraft_rust",
            Self::Model => "model",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
#[derive(Default)]
pub enum RustRaftBenchmarkHarnessKind {
    #[serde(rename = "full_reference_raft_harness")]
    FullReferenceRaftHarness,
    NativeKvbenchPartial,
    #[serde(rename = "rustraft_runtime")]
    RustRaftRuntime,
    Model,
    #[default]
    Unknown,
}

impl RustRaftBenchmarkHarnessKind {
    pub fn id(self) -> &'static str {
        match self {
            Self::FullReferenceRaftHarness => "full_reference_raft_harness",
            Self::NativeKvbenchPartial => "native_kvbench_partial",
            Self::RustRaftRuntime => "rustraft_runtime",
            Self::Model => "model",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(rename_all = "snake_case")]
pub enum RustRaftBenchmarkWorkload {
    SingleKeyWrites,
    BatchedWrites,
    ReplicationBatching,
    WalFsync,
    ReadIndexReads,
    LeaseReads,
    SnapshotInstallCatchup,
    SnapshotStreaming,
    LeaderTransferUnderLoad,
}

impl RustRaftBenchmarkWorkload {
    pub fn id(self) -> &'static str {
        match self {
            Self::SingleKeyWrites => "single_key_writes",
            Self::BatchedWrites => "batched_writes",
            Self::ReplicationBatching => "replication_batching",
            Self::WalFsync => "wal_fsync",
            Self::ReadIndexReads => "read_index_reads",
            Self::LeaseReads => "lease_reads",
            Self::SnapshotInstallCatchup => "snapshot_install_catchup",
            Self::SnapshotStreaming => "snapshot_streaming",
            Self::LeaderTransferUnderLoad => "leader_transfer_under_load",
        }
    }
}

pub fn rustraft_reference_raft_benchmark_workloads() -> Vec<RustRaftBenchmarkWorkload> {
    vec![
        RustRaftBenchmarkWorkload::SingleKeyWrites,
        RustRaftBenchmarkWorkload::BatchedWrites,
        RustRaftBenchmarkWorkload::ReplicationBatching,
        RustRaftBenchmarkWorkload::WalFsync,
        RustRaftBenchmarkWorkload::ReadIndexReads,
        RustRaftBenchmarkWorkload::LeaseReads,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup,
        RustRaftBenchmarkWorkload::SnapshotStreaming,
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad,
    ]
}

pub fn rustraft_reference_raft_benchmark_required_workloads() -> Vec<String> {
    rustraft_reference_raft_benchmark_workloads()
        .into_iter()
        .map(|workload| workload.id().to_string())
        .collect()
}

pub fn rustraft_production_readiness_input_with_benchmark_artifacts(
    mut input: RustRaftProductionReadinessInput,
    report: &RustRaftBenchmarkReport,
    summary: &RustRaftBenchmarkFailureSummary,
) -> Result<RustRaftProductionReadinessInput, String> {
    input.reference_raft_benchmark = Some(
        rustraft_reference_raft_benchmark_evidence_from_artifacts(report, summary)?,
    );
    Ok(input)
}

pub fn rustraft_production_readiness_input_with_benchmark_summary(
    mut input: RustRaftProductionReadinessInput,
    summary: &RustRaftBenchmarkFailureSummary,
) -> RustRaftProductionReadinessInput {
    input.reference_raft_benchmark = Some(rustraft_reference_raft_benchmark_evidence_from_summary(
        summary,
    ));
    input
}

pub fn rustraft_production_readiness_report_with_benchmark_artifacts(
    input: &RustRaftProductionReadinessInput,
    report: &RustRaftBenchmarkReport,
    summary: &RustRaftBenchmarkFailureSummary,
) -> Result<RustRaftProductionReadinessReport, String> {
    let input = rustraft_production_readiness_input_with_benchmark_artifacts(
        input.clone(),
        report,
        summary,
    )?;
    Ok(rustraft_production_readiness_report(&input))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkOptions {
    pub node_count: usize,
    pub iterations_per_workload: usize,
    pub batch_size: usize,
    pub payload_size_bytes: usize,
    pub pass_tolerance_percent: f64,
}

impl Default for RustRaftBenchmarkOptions {
    fn default() -> Self {
        Self {
            node_count: RUSTRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT,
            iterations_per_workload: 128,
            batch_size: 16,
            payload_size_bytes: RUSTRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES,
            pass_tolerance_percent: RUSTRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkSample {
    pub workload: RustRaftBenchmarkWorkload,
    pub engine: RustRaftBenchmarkEngine,
    pub engine_source: RustRaftBenchmarkEngineSource,
    #[serde(default)]
    pub benchmark_run_id: String,
    #[serde(default)]
    pub implementation: RustRaftBenchmarkImplementation,
    #[serde(default)]
    pub binary_path: Option<String>,
    #[serde(default)]
    pub git_revision: Option<String>,
    #[serde(default)]
    pub build_profile: String,
    #[serde(default)]
    pub harness_kind: RustRaftBenchmarkHarnessKind,
    pub node_count: usize,
    #[serde(default)]
    pub iterations_per_workload: usize,
    #[serde(default)]
    pub batch_size: usize,
    #[serde(default)]
    pub payload_size_bytes: usize,
    #[serde(default)]
    pub timed_iteration_count: usize,
    #[serde(default)]
    pub operations_per_timed_iteration: usize,
    #[serde(default)]
    pub total_duration_micros: u64,
    pub operation_count: usize,
    pub p50_latency_micros: u64,
    pub p99_latency_micros: u64,
    pub throughput_ops_per_sec: f64,
    pub correctness_passed: bool,
    #[serde(default)]
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkComparison {
    pub workload: RustRaftBenchmarkWorkload,
    pub reference_raft: RustRaftBenchmarkSample,
    pub rustraft: RustRaftBenchmarkSample,
    pub p50_ratio: f64,
    pub p99_ratio: f64,
    pub throughput_ratio: f64,
    pub passed: bool,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkReport {
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub generated_at_unix_ms: u64,
    #[serde(default)]
    pub benchmark_run_id: String,
    #[serde(default)]
    pub environment_fingerprint: String,
    pub node_count: usize,
    pub options: RustRaftBenchmarkOptions,
    pub pass_tolerance_percent: f64,
    pub correctness_required: bool,
    #[serde(default)]
    pub required_workloads: Vec<String>,
    pub passed: bool,
    pub comparisons: Vec<RustRaftBenchmarkComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkFailureSummary {
    #[serde(default)]
    pub schema: String,
    #[serde(default)]
    pub generated_at_unix_ms: u64,
    #[serde(default)]
    pub benchmark_run_id: String,
    #[serde(default)]
    pub environment_fingerprint: String,
    pub passed: bool,
    pub production_evidence_ready: bool,
    pub options: RustRaftBenchmarkOptions,
    #[serde(default)]
    pub required_workloads: Vec<String>,
    pub workload_count: usize,
    pub failed_workload_count: usize,
    pub workloads: Vec<RustRaftBenchmarkWorkloadSummary>,
    pub missing_reference_raft_binary_count: usize,
    pub unsupported_workload_count: usize,
    pub correctness_blocker_count: usize,
    pub performance_blocker_count: usize,
    pub uncategorized_blocker_count: usize,
    pub worst_p50_ratio: f64,
    pub worst_p99_ratio: f64,
    pub worst_throughput_ratio: f64,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RustRaftBenchmarkWorkloadSummary {
    pub workload: RustRaftBenchmarkWorkload,
    pub passed: bool,
    pub reference_raft_correctness_passed: bool,
    pub rustraft_correctness_passed: bool,
    pub reference_raft_engine_source: RustRaftBenchmarkEngineSource,
    pub rustraft_engine_source: RustRaftBenchmarkEngineSource,
    #[serde(default)]
    pub reference_raft_benchmark_run_id: String,
    #[serde(default)]
    pub rustraft_benchmark_run_id: String,
    #[serde(default)]
    pub reference_raft_implementation: RustRaftBenchmarkImplementation,
    #[serde(default)]
    pub rustraft_implementation: RustRaftBenchmarkImplementation,
    #[serde(default)]
    pub reference_raft_binary_path: Option<String>,
    #[serde(default)]
    pub rustraft_binary_path: Option<String>,
    #[serde(default)]
    pub reference_raft_git_revision: Option<String>,
    #[serde(default)]
    pub rustraft_git_revision: Option<String>,
    #[serde(default)]
    pub reference_raft_build_profile: String,
    #[serde(default)]
    pub rustraft_build_profile: String,
    #[serde(default)]
    pub reference_raft_harness_kind: RustRaftBenchmarkHarnessKind,
    #[serde(default)]
    pub rustraft_harness_kind: RustRaftBenchmarkHarnessKind,
    pub node_count: usize,
    #[serde(default)]
    pub reference_raft_node_count: usize,
    #[serde(default)]
    pub rustraft_node_count: usize,
    pub reference_raft_iterations_per_workload: usize,
    pub rustraft_iterations_per_workload: usize,
    pub reference_raft_batch_size: usize,
    pub rustraft_batch_size: usize,
    pub reference_raft_payload_size_bytes: usize,
    pub rustraft_payload_size_bytes: usize,
    #[serde(default)]
    pub reference_raft_timed_iteration_count: usize,
    #[serde(default)]
    pub rustraft_timed_iteration_count: usize,
    #[serde(default)]
    pub reference_raft_operations_per_timed_iteration: usize,
    #[serde(default)]
    pub rustraft_operations_per_timed_iteration: usize,
    #[serde(default)]
    pub reference_raft_total_duration_micros: u64,
    #[serde(default)]
    pub rustraft_total_duration_micros: u64,
    pub reference_raft_operation_count: usize,
    pub rustraft_operation_count: usize,
    #[serde(default)]
    pub reference_raft_p50_latency_micros: u64,
    #[serde(default)]
    pub rustraft_p50_latency_micros: u64,
    #[serde(default)]
    pub reference_raft_p99_latency_micros: u64,
    #[serde(default)]
    pub rustraft_p99_latency_micros: u64,
    #[serde(default)]
    pub reference_raft_throughput_ops_per_sec: f64,
    #[serde(default)]
    pub rustraft_throughput_ops_per_sec: f64,
    pub p50_ratio: f64,
    pub p99_ratio: f64,
    pub throughput_ratio: f64,
    pub blockers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RustRaftReferenceRaftNativeBenchmarkCapability {
    pub reference_raft_root: String,
    #[serde(default)]
    pub kvserver_binary_path: Option<String>,
    pub kvbench_binary_path: Option<String>,
    #[serde(default)]
    pub kvserver_source_path: Option<String>,
    pub kvbench_source_path: Option<String>,
    pub bench_script_path: Option<String>,
    #[serde(default)]
    pub cmake_kvserver_target_present: bool,
    pub cmake_kvbench_target_present: bool,
    pub supported_workloads: Vec<String>,
    pub missing_required_workloads: Vec<String>,
    pub blockers: Vec<String>,
}

pub trait RustRaftBenchmarkRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine;
    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::Model
    }
    fn binary_path(&self) -> Option<String> {
        None
    }
    fn git_revision(&self) -> Option<String> {
        None
    }
    fn build_profile(&self) -> String {
        "test".to_string()
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample;
}

#[derive(Debug, Clone)]
pub struct RustRaftSameMachineModelRunner {
    engine: RustRaftBenchmarkEngine,
}

impl RustRaftSameMachineModelRunner {
    pub fn reference_raft_baseline() -> Self {
        Self {
            engine: RustRaftBenchmarkEngine::ReferenceRaft,
        }
    }

    pub fn rustraft_candidate() -> Self {
        Self {
            engine: RustRaftBenchmarkEngine::RustRaft,
        }
    }
}

impl RustRaftBenchmarkRunner for RustRaftSameMachineModelRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        self.engine
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        run_same_machine_model_workload(self.engine, workload, options)
    }
}

#[derive(Debug, Clone)]
pub struct RustRaftRuntimeBenchmarkRunner {
    build_profile: String,
    git_revision: Option<String>,
}

impl RustRaftRuntimeBenchmarkRunner {
    pub fn new(build_profile: impl Into<String>) -> Self {
        Self {
            build_profile: build_profile.into(),
            git_revision: option_env!("VERGEN_GIT_SHA")
                .or(option_env!("GIT_HASH"))
                .map(str::to_string)
                .or_else(|| git_revision_for(Path::new(env!("CARGO_MANIFEST_DIR"))).ok()),
        }
    }
}

impl Default for RustRaftRuntimeBenchmarkRunner {
    fn default() -> Self {
        Self::new("debug")
    }
}

impl RustRaftBenchmarkRunner for RustRaftRuntimeBenchmarkRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        RustRaftBenchmarkEngine::RustRaft
    }

    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::RustRaftRuntime
    }

    fn binary_path(&self) -> Option<String> {
        std::env::current_exe()
            .ok()
            .map(|path| path.display().to_string())
    }

    fn git_revision(&self) -> Option<String> {
        self.git_revision.clone()
    }

    fn build_profile(&self) -> String {
        self.build_profile.clone()
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        run_rustraft_runtime_workload(workload, options, self)
    }
}

#[derive(Debug, Clone)]
pub struct RustRaftExternalReferenceRaftRunner {
    binary_path: PathBuf,
    reference_raft_root: Option<PathBuf>,
    git_revision: Option<String>,
    build_profile: String,
}

impl RustRaftExternalReferenceRaftRunner {
    pub fn new(
        binary_path: impl Into<PathBuf>,
        reference_raft_root: Option<impl Into<PathBuf>>,
        build_profile: impl Into<String>,
    ) -> Result<Self, String> {
        let binary_path = binary_path.into();
        if !binary_path.is_file() {
            return Err(format!(
                "benchmark:real_reference_raft_missing:{}",
                binary_path.display()
            ));
        }
        if let Some(blocker) = rustraft_reference_raft_harness_executable_blocker(&binary_path) {
            return Err(blocker);
        }
        let reference_raft_root = reference_raft_root.map(Into::into);
        let git_revision = reference_raft_root
            .as_ref()
            .and_then(|root| git_revision_for(root).ok());
        Ok(Self {
            binary_path,
            reference_raft_root,
            git_revision,
            build_profile: build_profile.into(),
        })
    }

    pub fn from_root(
        reference_raft_root: impl AsRef<Path>,
        build_profile: impl Into<String>,
    ) -> Result<Self, String> {
        let root = reference_raft_root.as_ref();
        let build_profile = build_profile.into();
        let binary = rustraft_find_or_build_reference_raft_harness(root, &build_profile)?;
        Self::new(binary, Some(root.to_path_buf()), build_profile)
    }
}

impl RustRaftBenchmarkRunner for RustRaftExternalReferenceRaftRunner {
    fn engine(&self) -> RustRaftBenchmarkEngine {
        RustRaftBenchmarkEngine::ReferenceRaft
    }

    fn engine_source(&self) -> RustRaftBenchmarkEngineSource {
        RustRaftBenchmarkEngineSource::RealReferenceRaft
    }

    fn binary_path(&self) -> Option<String> {
        Some(self.binary_path.display().to_string())
    }

    fn git_revision(&self) -> Option<String> {
        self.git_revision.clone()
    }

    fn build_profile(&self) -> String {
        self.build_profile.clone()
    }

    fn run_workload(
        &mut self,
        workload: RustRaftBenchmarkWorkload,
        options: &RustRaftBenchmarkOptions,
    ) -> RustRaftBenchmarkSample {
        let state_dir = temp_benchmark_dir(&format!("reference_raft-{}", workload.id()));
        let wal_dir = state_dir.join("wal");
        let snapshot_dir = state_dir.join("snapshot");
        if let Err(err) = fs::create_dir_all(&wal_dir) {
            return failed_real_reference_raft_sample(
                workload,
                options,
                self,
                format!(
                    "benchmark:real_reference_raft_state_dir_create_failed:{}:{err}",
                    workload.id()
                ),
            );
        }
        if let Err(err) = fs::create_dir_all(&snapshot_dir) {
            let _ = fs::remove_dir_all(&state_dir);
            return failed_real_reference_raft_sample(
                workload,
                options,
                self,
                format!(
                    "benchmark:real_reference_raft_state_dir_create_failed:{}:{err}",
                    workload.id()
                ),
            );
        }

        let mut command = Command::new(&self.binary_path);
        command
            .arg("--workload")
            .arg(workload.id())
            .arg("--node-count")
            .arg(options.node_count.to_string())
            .arg("--iterations")
            .arg(options.iterations_per_workload.to_string())
            .arg("--batch-size")
            .arg(options.batch_size.to_string())
            .arg("--payload-size-bytes")
            .arg(options.payload_size_bytes.to_string())
            .arg("--wal-dir")
            .arg(&wal_dir)
            .arg("--snapshot-dir")
            .arg(&snapshot_dir);
        if let Some(root) = &self.reference_raft_root {
            command.arg("--reference_raft-root").arg(root);
        }
        let output = match command.output() {
            Ok(output) => output,
            Err(err) => {
                let _ = fs::remove_dir_all(&state_dir);
                return failed_real_reference_raft_sample(
                    workload,
                    options,
                    self,
                    format!(
                        "benchmark:real_reference_raft_harness_spawn_failed:{}:{err}",
                        workload.id()
                    ),
                );
            }
        };
        if !output.status.success() {
            let _ = fs::remove_dir_all(&state_dir);
            let status = output
                .status
                .code()
                .map(|code| format!("exit_{code}"))
                .unwrap_or_else(|| "signal".to_string());
            return failed_real_reference_raft_sample(
                workload,
                options,
                self,
                format!(
                    "benchmark:real_reference_raft_harness_failed:{}:{}:{}",
                    workload.id(),
                    status,
                    String::from_utf8_lossy(&output.stderr).trim()
                ),
            );
        }
        let mut sample: RustRaftBenchmarkSample = match serde_json::from_slice(&output.stdout) {
            Ok(sample) => sample,
            Err(err) => {
                let _ = fs::remove_dir_all(&state_dir);
                return failed_real_reference_raft_sample(
                    workload,
                    options,
                    self,
                    format!(
                        "benchmark:real_reference_raft_harness_invalid_json:{}:{err}",
                        workload.id()
                    ),
                );
            }
        };
        let _ = fs::remove_dir_all(&state_dir);
        apply_external_reference_raft_sample_identity_validation(&mut sample, workload);
        sample.workload = workload;
        sample.engine = RustRaftBenchmarkEngine::ReferenceRaft;
        sample.engine_source = RustRaftBenchmarkEngineSource::RealReferenceRaft;
        apply_external_reference_raft_sample_provenance_validation(&mut sample, workload, self);
        sample.git_revision = sample.git_revision.or_else(|| self.git_revision());
        if sample.harness_kind == RustRaftBenchmarkHarnessKind::Unknown
            && sample.build_profile == "native-kvbench-partial"
        {
            sample.harness_kind = RustRaftBenchmarkHarnessKind::NativeKvbenchPartial;
        }
        apply_external_reference_raft_sample_shape_validation(&mut sample, workload, options);
        sample
    }
}

fn apply_external_reference_raft_sample_identity_validation(
    sample: &mut RustRaftBenchmarkSample,
    expected_workload: RustRaftBenchmarkWorkload,
) {
    if sample.workload != expected_workload {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_workload_mismatch:{}:{}",
            expected_workload.id(),
            sample.workload.id()
        ));
    }
    if sample.engine != RustRaftBenchmarkEngine::ReferenceRaft {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_engine_mismatch:{}:{:?}",
            expected_workload.id(),
            sample.engine
        ));
    }
    if sample.engine_source != RustRaftBenchmarkEngineSource::RealReferenceRaft {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_engine_source_mismatch:{}:{:?}",
            expected_workload.id(),
            sample.engine_source
        ));
    }
    if sample.implementation != RustRaftBenchmarkImplementation::ReferenceRaftCpp {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_implementation_mismatch:{}:{}:reference_raft_cpp",
            expected_workload.id(),
            sample.implementation.id()
        ));
    }
}

fn apply_external_reference_raft_sample_provenance_validation(
    sample: &mut RustRaftBenchmarkSample,
    expected_workload: RustRaftBenchmarkWorkload,
    runner: &RustRaftExternalReferenceRaftRunner,
) {
    let expected_binary = runner.binary_path();
    if sample.binary_path.is_some() && sample.binary_path != expected_binary {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_binary_path_mismatch:{}",
            expected_workload.id()
        ));
    }
    sample.binary_path = expected_binary;

    let expected_git_revision = runner.git_revision();
    if sample.git_revision.is_some()
        && expected_git_revision.is_some()
        && sample.git_revision != expected_git_revision
    {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_git_revision_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.git_revision.clone().unwrap_or_default(),
            expected_git_revision.clone().unwrap_or_default()
        ));
    }
    if expected_git_revision.is_some() {
        sample.git_revision = expected_git_revision;
    }

    let expected_build_profile = runner.build_profile();
    if !sample.build_profile.is_empty() && sample.build_profile != expected_build_profile {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_build_profile_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.build_profile,
            expected_build_profile
        ));
    }
    sample.build_profile = expected_build_profile;
}

fn apply_external_reference_raft_sample_shape_validation(
    sample: &mut RustRaftBenchmarkSample,
    expected_workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) {
    let expected_operation_count = operation_count_for(expected_workload, options);
    if sample.node_count != options.node_count {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_node_count_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.node_count,
            options.node_count
        ));
    }
    if sample.iterations_per_workload != options.iterations_per_workload {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_iterations_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.iterations_per_workload,
            options.iterations_per_workload
        ));
    }
    if sample.batch_size != options.batch_size {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_batch_size_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.batch_size,
            options.batch_size
        ));
    }
    if sample.payload_size_bytes != options.payload_size_bytes {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_payload_size_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.payload_size_bytes,
            options.payload_size_bytes
        ));
    }
    if sample.operation_count != expected_operation_count {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_operation_count_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.operation_count,
            expected_operation_count
        ));
    }
    if sample.timed_iteration_count != options.iterations_per_workload {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_timed_iteration_count_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.timed_iteration_count,
            options.iterations_per_workload
        ));
    }
    let expected_operations_per_timed_iteration = writes_per_iteration(expected_workload, options);
    if sample.operations_per_timed_iteration != expected_operations_per_timed_iteration {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_operations_per_timed_iteration_mismatch:{}:{}:{}",
            expected_workload.id(),
            sample.operations_per_timed_iteration,
            expected_operations_per_timed_iteration
        ));
    }
    if sample.total_duration_micros == 0 {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_total_duration_zero:{}",
            expected_workload.id()
        ));
    } else if sample.total_duration_micros < sample.timed_iteration_count as u64 {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_total_duration_below_timed_iterations:{}:{}:{}",
            expected_workload.id(),
            sample.total_duration_micros,
            sample.timed_iteration_count
        ));
    } else if !throughput_matches_duration(
        sample.operation_count,
        sample.total_duration_micros,
        sample.throughput_ops_per_sec,
    ) {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_throughput_duration_mismatch:{}:{:.6}:{:.6}",
            expected_workload.id(),
            sample.throughput_ops_per_sec,
            throughput_from_duration(sample.operation_count, sample.total_duration_micros)
        ));
    }
    if sample.p50_latency_micros > sample.total_duration_micros {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_p50_exceeds_total_duration:{}:{}:{}",
            expected_workload.id(),
            sample.p50_latency_micros,
            sample.total_duration_micros
        ));
    }
    if sample.p99_latency_micros > sample.total_duration_micros {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_p99_exceeds_total_duration:{}:{}:{}",
            expected_workload.id(),
            sample.p99_latency_micros,
            sample.total_duration_micros
        ));
    }
    if sample.p50_latency_micros == 0 {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_p50_latency_zero:{}",
            expected_workload.id()
        ));
    }
    if sample.p99_latency_micros == 0 {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_p99_latency_zero:{}",
            expected_workload.id()
        ));
    }
    if sample.p50_latency_micros > 0
        && sample.p99_latency_micros > 0
        && sample.p99_latency_micros < sample.p50_latency_micros
    {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_latency_order_invalid:{}:{}:{}",
            expected_workload.id(),
            sample.p50_latency_micros,
            sample.p99_latency_micros
        ));
    }
    if !sample.throughput_ops_per_sec.is_finite() || sample.throughput_ops_per_sec <= 0.0 {
        sample.blockers.push(format!(
            "benchmark:real_reference_raft_harness_throughput_invalid:{}",
            expected_workload.id()
        ));
    }
    sample.blockers.sort();
    sample.blockers.dedup();
    if !sample.blockers.is_empty() {
        sample.correctness_passed = false;
    }
}

fn failed_real_reference_raft_sample(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
    runner: &RustRaftExternalReferenceRaftRunner,
    blocker: String,
) -> RustRaftBenchmarkSample {
    RustRaftBenchmarkSample {
        workload,
        engine: RustRaftBenchmarkEngine::ReferenceRaft,
        engine_source: RustRaftBenchmarkEngineSource::RealReferenceRaft,
        benchmark_run_id: String::new(),
        implementation: RustRaftBenchmarkImplementation::ReferenceRaftCpp,
        binary_path: runner.binary_path(),
        git_revision: runner.git_revision(),
        build_profile: runner.build_profile(),
        harness_kind: RustRaftBenchmarkHarnessKind::FullReferenceRaftHarness,
        node_count: options.node_count,
        iterations_per_workload: options.iterations_per_workload,
        batch_size: options.batch_size,
        payload_size_bytes: options.payload_size_bytes,
        timed_iteration_count: options.iterations_per_workload,
        operations_per_timed_iteration: writes_per_iteration(workload, options),
        total_duration_micros: 1_000_000_000,
        operation_count: operation_count_for(workload, options),
        p50_latency_micros: 1_000_000_000,
        p99_latency_micros: 1_000_000_000,
        throughput_ops_per_sec: 1.0,
        correctness_passed: false,
        blockers: vec![blocker],
    }
}

pub fn rustraft_find_reference_raft_harness(
    reference_raft_root: impl AsRef<Path>,
) -> Result<PathBuf, String> {
    let root = reference_raft_root.as_ref();
    if !root.is_dir() {
        return Err(format!(
            "benchmark:real_reference_raft_missing:{}",
            root.display()
        ));
    }
    let candidates = [
        root.join("target/release/reference_raft_parity_benchmark"),
        root.join("target/debug/reference_raft_parity_benchmark"),
        root.join("build/reference_raft_parity_benchmark"),
        root.join("bin/reference_raft_parity_benchmark"),
        root.join("reference_raft_parity_benchmark"),
    ];
    let mut non_executable = Vec::new();
    for path in candidates {
        if !path.is_file() {
            continue;
        }
        if let Some(blocker) = rustraft_reference_raft_harness_executable_blocker(&path) {
            non_executable.push(blocker);
            continue;
        }
        return Ok(path);
    }
    if non_executable.is_empty() {
        Err(format!(
            "benchmark:real_reference_raft_missing:{}",
            root.display()
        ))
    } else {
        Err(non_executable.join(";"))
    }
}

#[cfg(unix)]
fn rustraft_reference_raft_harness_executable_blocker(path: &Path) -> Option<String> {
    use std::os::unix::fs::PermissionsExt;

    match fs::metadata(path) {
        Ok(metadata) if metadata.permissions().mode() & 0o111 != 0 => None,
        Ok(_) => Some(format!(
            "benchmark:real_reference_raft_harness_not_executable:{}",
            path.display()
        )),
        Err(err) => Some(format!(
            "benchmark:real_reference_raft_harness_metadata_failed:{}:{err}",
            path.display()
        )),
    }
}

#[cfg(not(unix))]
fn rustraft_reference_raft_harness_executable_blocker(_path: &Path) -> Option<String> {
    None
}

pub fn rustraft_probe_reference_raft_native_benchmark(
    reference_raft_root: impl AsRef<Path>,
) -> RustRaftReferenceRaftNativeBenchmarkCapability {
    let root = reference_raft_root.as_ref();
    let kvserver_binary = rustraft_find_reference_raft_kvserver(root).ok();
    let kvbench_binary = rustraft_find_reference_raft_kvbench(root).ok();
    let kvserver_source = root.join("example/kv/kv_server.cc");
    let kvbench_source = root.join("example/kv/kv_benchmark.cc");
    let bench_script = root.join("script/bench.sh");
    let cmake_kv = root.join("example/kv/CMakeLists.txt");
    let cmake_kvserver_target_present = fs::read_to_string(&cmake_kv)
        .map(|text| text.contains("add_executable(kvserver"))
        .unwrap_or(false);
    let cmake_kvbench_target_present = fs::read_to_string(&cmake_kv)
        .map(|text| text.contains("add_executable(kvbench"))
        .unwrap_or(false);

    let mut supported_workloads = Vec::new();
    if kvserver_binary.is_some() && kvbench_binary.is_some() {
        supported_workloads.push(RustRaftBenchmarkWorkload::SingleKeyWrites.id().to_string());
        supported_workloads.push(RustRaftBenchmarkWorkload::BatchedWrites.id().to_string());
        supported_workloads.push(
            RustRaftBenchmarkWorkload::ReplicationBatching
                .id()
                .to_string(),
        );
        supported_workloads.push(RustRaftBenchmarkWorkload::ReadIndexReads.id().to_string());
        supported_workloads.push(RustRaftBenchmarkWorkload::LeaseReads.id().to_string());
    }

    let required = rustraft_reference_raft_benchmark_workloads()
        .into_iter()
        .map(|workload| workload.id().to_string())
        .collect::<Vec<_>>();
    let missing_required_workloads = required
        .into_iter()
        .filter(|workload| !supported_workloads.contains(workload))
        .collect::<Vec<_>>();

    let mut blockers = Vec::new();
    if !root.is_dir() {
        blockers.push("benchmark:real_reference_raft_missing".to_string());
    }
    if kvserver_binary.is_none() {
        blockers.push("benchmark:reference_raft_kvserver_binary_missing".to_string());
    }
    if kvbench_binary.is_none() {
        blockers.push("benchmark:reference_raft_kvbench_binary_missing".to_string());
    }
    if !missing_required_workloads.is_empty() {
        blockers.push("benchmark:reference_raft_native_kvbench_partial".to_string());
    }
    blockers.extend(
        missing_required_workloads
            .iter()
            .map(|workload| format!("benchmark:workload_missing:{workload}")),
    );

    RustRaftReferenceRaftNativeBenchmarkCapability {
        reference_raft_root: root.display().to_string(),
        kvserver_binary_path: kvserver_binary.map(|path| path.display().to_string()),
        kvbench_binary_path: kvbench_binary.map(|path| path.display().to_string()),
        kvserver_source_path: kvserver_source
            .is_file()
            .then(|| kvserver_source.display().to_string()),
        kvbench_source_path: kvbench_source
            .is_file()
            .then(|| kvbench_source.display().to_string()),
        bench_script_path: bench_script
            .is_file()
            .then(|| bench_script.display().to_string()),
        cmake_kvserver_target_present,
        cmake_kvbench_target_present,
        supported_workloads,
        missing_required_workloads,
        blockers,
    }
}

fn rustraft_find_reference_raft_kvserver(reference_raft_root: &Path) -> Result<PathBuf, String> {
    let root = reference_raft_root;
    let candidates = [
        root.join("build/example/kv/kvserver"),
        root.join("build/example/kv/kv_server"),
        root.join("bin/kvserver"),
        root.join("kvserver"),
    ];
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            format!(
                "benchmark:reference_raft_kvserver_binary_missing:{}",
                root.display()
            )
        })
}

fn rustraft_find_reference_raft_kvbench(reference_raft_root: &Path) -> Result<PathBuf, String> {
    let root = reference_raft_root;
    let candidates = [
        root.join("build/example/kv/kvbench"),
        root.join("build/example/kv/kv_benchmark"),
        root.join("bin/kvbench"),
        root.join("kvbench"),
    ];
    candidates
        .into_iter()
        .find(|path| path.is_file())
        .ok_or_else(|| {
            format!(
                "benchmark:reference_raft_kvbench_binary_missing:{}",
                root.display()
            )
        })
}

pub fn rustraft_find_or_build_reference_raft_harness(
    reference_raft_root: impl AsRef<Path>,
    build_profile: &str,
) -> Result<PathBuf, String> {
    let root = reference_raft_root.as_ref();
    let initial_harness_error = match rustraft_find_reference_raft_harness(root) {
        Ok(binary) => return Ok(binary),
        Err(err) => err,
    };
    if !root.is_dir() {
        return Err(format!(
            "benchmark:real_reference_raft_missing:{}",
            root.display()
        ));
    }

    let build_attempts = [
        try_reference_raft_build_script(
            root,
            &root.join("scripts/build_reference_raft_parity_benchmark.sh"),
            build_profile,
        ),
        try_reference_raft_build_script(
            root,
            &root.join("build_reference_raft_parity_benchmark.sh"),
            build_profile,
        ),
        try_reference_raft_cmake_target(root, build_profile),
        try_reference_raft_bazel_target(root, build_profile),
    ];

    let final_harness_error = match rustraft_find_reference_raft_harness(root) {
        Ok(binary) => return Ok(binary),
        Err(err) => err,
    };

    let attempted = build_attempts
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if attempted.is_empty() {
        Err(final_harness_error)
    } else {
        Err(format!(
            "{}; initial_harness_error={}; build_attempts={}",
            final_harness_error,
            initial_harness_error,
            attempted.join("|")
        ))
    }
}

fn try_reference_raft_build_script(
    root: &Path,
    script: &Path,
    build_profile: &str,
) -> Result<(), String> {
    if !script.is_file() {
        return Ok(());
    }
    let status = Command::new("bash")
        .arg(script)
        .arg("--profile")
        .arg(build_profile)
        .current_dir(root)
        .status()
        .map_err(|err| format!("build_script:{}:{err}", script.display()))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("build_script:{}:{status}", script.display()))
    }
}

fn try_reference_raft_cmake_target(root: &Path, build_profile: &str) -> Result<(), String> {
    let cmake_file = root.join("CMakeLists.txt");
    let Ok(cmake_text) = fs::read_to_string(&cmake_file) else {
        return Ok(());
    };
    if !cmake_text.contains("reference_raft_parity_benchmark") {
        return Ok(());
    }
    let build_dir = root.join("build");
    let build_type = if build_profile == "release" {
        "Release"
    } else {
        "Debug"
    };
    let configure = Command::new("cmake")
        .arg("-S")
        .arg(root)
        .arg("-B")
        .arg(&build_dir)
        .arg(format!("-DCMAKE_BUILD_TYPE={build_type}"))
        .current_dir(root)
        .status()
        .map_err(|err| format!("cmake_configure:{err}"))?;
    if !configure.success() {
        return Err(format!("cmake_configure:{configure}"));
    }
    let build = Command::new("cmake")
        .arg("--build")
        .arg(&build_dir)
        .arg("--target")
        .arg("reference_raft_parity_benchmark")
        .current_dir(root)
        .status()
        .map_err(|err| format!("cmake_build:{err}"))?;
    if build.success() {
        Ok(())
    } else {
        Err(format!("cmake_build:{build}"))
    }
}

fn try_reference_raft_bazel_target(root: &Path, _build_profile: &str) -> Result<(), String> {
    let build_file = root.join("BUILD");
    let Ok(build_text) = fs::read_to_string(&build_file) else {
        return Ok(());
    };
    if !build_text.contains("reference_raft_parity_benchmark") {
        return Ok(());
    }
    let status = Command::new("bazel")
        .arg("build")
        .arg("//:reference_raft_parity_benchmark")
        .current_dir(root)
        .status()
        .map_err(|err| format!("bazel_build:{err}"))?;
    if status.success() {
        let bazel_binary = root.join("bazel-bin/reference_raft_parity_benchmark");
        let bin_dir = root.join("bin");
        fs::create_dir_all(&bin_dir).map_err(|err| format!("bazel_bin_dir:{err}"))?;
        if bazel_binary.is_file() {
            fs::copy(
                &bazel_binary,
                bin_dir.join("reference_raft_parity_benchmark"),
            )
            .map_err(|err| format!("bazel_copy:{err}"))?;
        }
        Ok(())
    } else {
        Err(format!("bazel_build:{status}"))
    }
}

pub fn rustraft_run_reference_raft_parity_benchmark(
    reference_raft: &mut impl RustRaftBenchmarkRunner,
    rustraft: &mut impl RustRaftBenchmarkRunner,
    options: &RustRaftBenchmarkOptions,
) -> RustRaftBenchmarkReport {
    assert_eq!(
        reference_raft.engine(),
        RustRaftBenchmarkEngine::ReferenceRaft
    );
    assert_eq!(rustraft.engine(), RustRaftBenchmarkEngine::RustRaft);
    let benchmark_run_id = benchmark_run_id();
    let comparisons = rustraft_reference_raft_benchmark_workloads()
        .into_iter()
        .map(|workload| {
            let mut baseline = reference_raft.run_workload(workload, options);
            let mut candidate = rustraft.run_workload(workload, options);
            baseline.benchmark_run_id = benchmark_run_id.clone();
            candidate.benchmark_run_id = benchmark_run_id.clone();
            compare_samples(baseline, candidate, options.pass_tolerance_percent)
        })
        .collect::<Vec<_>>();
    let passed = comparisons.len() == rustraft_reference_raft_benchmark_workloads().len()
        && comparisons.iter().all(|comparison| comparison.passed);
    RustRaftBenchmarkReport {
        schema: RUSTRAFT_BENCHMARK_REPORT_SCHEMA.to_string(),
        generated_at_unix_ms: benchmark_now_unix_ms(),
        benchmark_run_id,
        environment_fingerprint: benchmark_environment_fingerprint(),
        node_count: options.node_count,
        options: options.clone(),
        pass_tolerance_percent: options.pass_tolerance_percent,
        correctness_required: true,
        required_workloads: rustraft_reference_raft_benchmark_required_workloads(),
        passed,
        comparisons,
    }
}

pub fn rustraft_assert_reference_raft_parity(
    report: &RustRaftBenchmarkReport,
) -> Result<(), String> {
    if report.passed {
        return Ok(());
    }
    let blockers = report
        .comparisons
        .iter()
        .filter(|comparison| !comparison.passed)
        .flat_map(|comparison| {
            comparison
                .blockers
                .iter()
                .map(move |blocker| format!("{}:{blocker}", comparison.workload.id()))
        })
        .collect::<Vec<_>>();
    Err(blockers.join("; "))
}

pub fn rustraft_assert_production_reference_raft_parity(
    report: &RustRaftBenchmarkReport,
) -> Result<(), String> {
    let parity_error = rustraft_assert_reference_raft_parity(report).err();
    let evidence = rustraft_reference_raft_benchmark_evidence(report);
    if evidence.real_reference_raft
        && evidence.rustraft_runtime
        && evidence.correctness_passed
        && evidence.performance_within_threshold
        && evidence.blockers.is_empty()
    {
        return Ok(());
    }
    let mut blockers = evidence.blockers;
    if let Some(error) = parity_error {
        blockers.extend(
            error
                .split("; ")
                .filter(|blocker| !blocker.is_empty())
                .map(str::to_string),
        );
    }
    blockers.sort();
    blockers.dedup();
    Err(blockers.join("; "))
}

pub fn rustraft_assert_production_reference_raft_artifacts(
    report: &RustRaftBenchmarkReport,
    summary: &RustRaftBenchmarkFailureSummary,
) -> Result<(), String> {
    let expected = rustraft_reference_raft_benchmark_failure_summary(report);
    if summary != &expected {
        return Err("benchmark:summary_artifact_mismatch".to_string());
    }
    let mut blockers = Vec::new();
    if let Err(error) = rustraft_assert_production_reference_raft_summary(summary) {
        blockers.extend(error.split("; ").map(str::to_string));
    }
    if let Err(error) = rustraft_assert_production_reference_raft_parity(report) {
        blockers.extend(error.split("; ").map(str::to_string));
    }
    blockers.sort();
    blockers.dedup();
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(blockers.join("; "))
    }
}

pub fn rustraft_assert_production_reference_raft_summary(
    summary: &RustRaftBenchmarkFailureSummary,
) -> Result<(), String> {
    let mut blockers = Vec::new();
    if summary.schema != RUSTRAFT_BENCHMARK_SUMMARY_SCHEMA {
        blockers.push(format!(
            "benchmark:summary_schema_mismatch:{}:{}",
            summary.schema, RUSTRAFT_BENCHMARK_SUMMARY_SCHEMA
        ));
    }
    blockers.extend(benchmark_artifact_timestamp_blockers(
        "summary",
        summary.generated_at_unix_ms,
    ));
    if summary.benchmark_run_id.is_empty() {
        blockers.push("benchmark:summary_run_id_missing".to_string());
    }
    if summary.environment_fingerprint.is_empty() {
        blockers.push("benchmark:summary_environment_fingerprint_missing".to_string());
    }
    blockers.extend(benchmark_environment_release_blockers(
        "summary",
        &summary.environment_fingerprint,
    ));
    if !summary.passed {
        blockers.push("benchmark:summary_report_failed".to_string());
    }
    if !summary.production_evidence_ready {
        blockers.push("benchmark:summary_production_evidence_not_ready".to_string());
    }
    if summary.workload_count == 0 {
        blockers.push("benchmark:summary_missing_workloads".to_string());
    }
    blockers.extend(benchmark_production_option_blockers(&summary.options));
    let actual_failed_workload_count = summary
        .workloads
        .iter()
        .filter(|workload| !workload.passed)
        .count();
    let actual_passed =
        benchmark_summary_has_required_workload_set(summary) && actual_failed_workload_count == 0;
    if summary.passed != actual_passed {
        blockers.push(format!(
            "benchmark:summary_passed_mismatch:declared_{}_actual_{}",
            summary.passed, actual_passed
        ));
    }
    if summary.failed_workload_count != actual_failed_workload_count {
        blockers.push(format!(
            "benchmark:summary_failed_workload_count_mismatch:declared_{}_actual_{}",
            summary.failed_workload_count, actual_failed_workload_count
        ));
    }
    let required_workload_manifest = rustraft_reference_raft_benchmark_required_workloads();
    if summary.required_workloads != required_workload_manifest {
        blockers.push(format!(
            "benchmark:summary_required_workloads_mismatch:declared_{}_required_{}",
            summary.required_workloads.join(","),
            required_workload_manifest.join(",")
        ));
    }
    let observed_workload_order = summary
        .workloads
        .iter()
        .map(|workload| workload.workload.id().to_string())
        .collect::<Vec<_>>();
    if observed_workload_order != required_workload_manifest {
        blockers.push(format!(
            "benchmark:summary_workload_order_mismatch:observed_{}_required_{}",
            observed_workload_order.join(","),
            required_workload_manifest.join(",")
        ));
    }
    let required_workloads = required_workload_manifest
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut observed_workloads = std::collections::BTreeSet::new();
    let mut duplicate_workloads = std::collections::BTreeSet::new();
    for workload in &summary.workloads {
        let workload_id = workload.workload.id().to_string();
        if !observed_workloads.insert(workload_id.clone()) {
            duplicate_workloads.insert(workload_id);
        }
    }
    if summary.workload_count != summary.workloads.len() {
        blockers.push(format!(
            "benchmark:summary_workload_count_mismatch:declared_{}_actual_{}",
            summary.workload_count,
            summary.workloads.len()
        ));
    }
    if summary.workload_count != required_workloads.len() {
        blockers.push(format!(
            "benchmark:summary_required_workload_count_mismatch:declared_{}_required_{}",
            summary.workload_count,
            required_workloads.len()
        ));
    }
    for workload in required_workloads.difference(&observed_workloads) {
        blockers.push(format!(
            "benchmark:summary_required_workload_missing:{workload}"
        ));
    }
    for workload in duplicate_workloads {
        blockers.push(format!("benchmark:summary_duplicate_workload:{workload}"));
    }
    if summary.failed_workload_count > 0 {
        blockers.push(format!(
            "benchmark:summary_failed_workloads:{}",
            summary.failed_workload_count
        ));
    }
    if summary.missing_reference_raft_binary_count > 0 {
        blockers.push(format!(
            "benchmark:summary_missing_reference_raft_binaries:{}",
            summary.missing_reference_raft_binary_count
        ));
    }
    if summary.unsupported_workload_count > 0 {
        blockers.push(format!(
            "benchmark:summary_unsupported_workloads:{}",
            summary.unsupported_workload_count
        ));
    }
    if summary.correctness_blocker_count > 0 {
        blockers.push(format!(
            "benchmark:summary_correctness_blockers:{}",
            summary.correctness_blocker_count
        ));
    }
    if summary.performance_blocker_count > 0 {
        blockers.push(format!(
            "benchmark:summary_performance_blockers:{}",
            summary.performance_blocker_count
        ));
    }
    if summary.uncategorized_blocker_count > 0 {
        blockers.push(format!(
            "benchmark:summary_uncategorized_blockers:{}",
            summary.uncategorized_blocker_count
        ));
    }
    let expected_worst_p50_ratio = summary
        .workloads
        .iter()
        .map(|workload| workload.p50_ratio)
        .fold(0.0, f64::max);
    let expected_worst_p99_ratio = summary
        .workloads
        .iter()
        .map(|workload| workload.p99_ratio)
        .fold(0.0, f64::max);
    let expected_worst_throughput_ratio = summary
        .workloads
        .iter()
        .map(|workload| workload.throughput_ratio)
        .fold(f64::INFINITY, f64::min);
    let expected_worst_throughput_ratio = if expected_worst_throughput_ratio.is_finite() {
        expected_worst_throughput_ratio
    } else {
        0.0
    };
    push_summary_ratio_finite_blocker(
        &mut blockers,
        "worst_p50",
        summary.worst_p50_ratio,
        expected_worst_p50_ratio,
    );
    push_summary_ratio_finite_blocker(
        &mut blockers,
        "worst_p99",
        summary.worst_p99_ratio,
        expected_worst_p99_ratio,
    );
    push_summary_ratio_finite_blocker(
        &mut blockers,
        "worst_throughput",
        summary.worst_throughput_ratio,
        expected_worst_throughput_ratio,
    );
    push_summary_ratio_mismatch(
        &mut blockers,
        "worst_p50",
        summary.worst_p50_ratio,
        expected_worst_p50_ratio,
    );
    push_summary_ratio_mismatch(
        &mut blockers,
        "worst_p99",
        summary.worst_p99_ratio,
        expected_worst_p99_ratio,
    );
    push_summary_ratio_mismatch(
        &mut blockers,
        "worst_throughput",
        summary.worst_throughput_ratio,
        expected_worst_throughput_ratio,
    );
    for workload in &summary.workloads {
        let max_latency_ratio = 1.0 + summary.options.pass_tolerance_percent / 100.0;
        let min_throughput_ratio = 1.0 - summary.options.pass_tolerance_percent / 100.0;
        let workload_performance_passed = workload.p50_ratio.is_finite()
            && workload.p99_ratio.is_finite()
            && workload.throughput_ratio.is_finite()
            && workload.p50_ratio <= max_latency_ratio
            && workload.p99_ratio <= max_latency_ratio
            && workload.throughput_ratio >= min_throughput_ratio;
        let workload_correctness_passed =
            workload.reference_raft_correctness_passed && workload.rustraft_correctness_passed;
        if workload.passed && !workload.blockers.is_empty() {
            blockers.push(format!(
                "benchmark:summary_workload_passed_with_blockers:{}",
                workload.workload.id()
            ));
        }
        if workload.passed && !workload_performance_passed {
            blockers.push(format!(
                "benchmark:summary_workload_passed_despite_regression:{}",
                workload.workload.id()
            ));
        }
        if !workload.passed
            && workload.blockers.is_empty()
            && workload_correctness_passed
            && workload_performance_passed
        {
            blockers.push(format!(
                "benchmark:summary_workload_failed_without_blockers:{}",
                workload.workload.id()
            ));
        }
        if !workload.passed {
            blockers.push(format!(
                "benchmark:summary_workload_failed:{}",
                workload.workload.id()
            ));
        }
        if !workload.reference_raft_correctness_passed {
            blockers.push(format!(
                "benchmark:summary_reference_raft_correctness_failed:{}",
                workload.workload.id()
            ));
        }
        if !workload.rustraft_correctness_passed {
            blockers.push(format!(
                "benchmark:summary_rustraft_correctness_failed:{}",
                workload.workload.id()
            ));
        }
        if workload.reference_raft_engine_source != RustRaftBenchmarkEngineSource::RealReferenceRaft
        {
            blockers.push(format!(
                "benchmark:summary_real_reference_raft_missing:{}",
                workload.workload.id()
            ));
        }
        if workload.rustraft_engine_source != RustRaftBenchmarkEngineSource::RustRaftRuntime {
            blockers.push(format!(
                "benchmark:summary_rustraft_runtime_missing:{}",
                workload.workload.id()
            ));
        }
        if workload.reference_raft_benchmark_run_id.is_empty() {
            blockers.push(format!(
                "benchmark:summary_reference_raft_run_id_missing:{}",
                workload.workload.id()
            ));
        } else if workload.reference_raft_benchmark_run_id != summary.benchmark_run_id {
            blockers.push(format!(
                "benchmark:summary_reference_raft_run_id_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_benchmark_run_id,
                summary.benchmark_run_id
            ));
        }
        if workload.rustraft_benchmark_run_id.is_empty() {
            blockers.push(format!(
                "benchmark:summary_rustraft_run_id_missing:{}",
                workload.workload.id()
            ));
        } else if workload.rustraft_benchmark_run_id != summary.benchmark_run_id {
            blockers.push(format!(
                "benchmark:summary_rustraft_run_id_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_benchmark_run_id,
                summary.benchmark_run_id
            ));
        }
        if !workload.reference_raft_benchmark_run_id.is_empty()
            && !workload.rustraft_benchmark_run_id.is_empty()
            && workload.reference_raft_benchmark_run_id != workload.rustraft_benchmark_run_id
        {
            blockers.push(format!(
                "benchmark:summary_sample_run_id_pair_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_benchmark_run_id,
                workload.rustraft_benchmark_run_id
            ));
        }
        if workload.reference_raft_implementation
            != RustRaftBenchmarkImplementation::ReferenceRaftCpp
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_implementation_mismatch:{}:{}:reference_raft_cpp",
                workload.workload.id(),
                workload.reference_raft_implementation.id()
            ));
        }
        if workload.rustraft_implementation != RustRaftBenchmarkImplementation::RustRaftRust {
            blockers.push(format!(
                "benchmark:summary_rustraft_implementation_mismatch:{}:{}:rustraft_rust",
                workload.workload.id(),
                workload.rustraft_implementation.id()
            ));
        }
        blockers.extend(summary_binary_path_blockers(
            "reference_raft",
            workload.workload,
            workload.reference_raft_binary_path.as_deref(),
        ));
        blockers.extend(summary_binary_path_blockers(
            "rustraft",
            workload.workload,
            workload.rustraft_binary_path.as_deref(),
        ));
        blockers.extend(summary_git_revision_blockers(
            "reference_raft",
            workload.workload,
            workload.reference_raft_git_revision.as_deref(),
        ));
        blockers.extend(summary_git_revision_blockers(
            "rustraft",
            workload.workload,
            workload.rustraft_git_revision.as_deref(),
        ));
        if workload.reference_raft_build_profile.is_empty() {
            blockers.push(format!(
                "benchmark:summary_reference_raft_provenance_build_profile_missing:{}",
                workload.workload.id()
            ));
        }
        if workload.rustraft_build_profile.is_empty() {
            blockers.push(format!(
                "benchmark:summary_rustraft_provenance_build_profile_missing:{}",
                workload.workload.id()
            ));
        }
        if !workload.reference_raft_build_profile.is_empty()
            && !workload.rustraft_build_profile.is_empty()
            && workload.reference_raft_build_profile != workload.rustraft_build_profile
        {
            blockers.push(format!(
                "benchmark:summary_build_profile_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_build_profile,
                workload.rustraft_build_profile
            ));
        }
        if let (Some(reference_raft_binary_path), Some(rustraft_binary_path)) = (
            workload.reference_raft_binary_path.as_deref(),
            workload.rustraft_binary_path.as_deref(),
        ) {
            if !reference_raft_binary_path.is_empty()
                && reference_raft_binary_path == rustraft_binary_path
            {
                blockers.push(format!(
                    "benchmark:summary_binary_path_collision:{}:{}",
                    workload.workload.id(),
                    reference_raft_binary_path
                ));
            }
        }
        if !workload.reference_raft_build_profile.is_empty()
            && workload.reference_raft_build_profile != "release"
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_build_profile_not_release:{}:{}",
                workload.workload.id(),
                workload.reference_raft_build_profile
            ));
        }
        if !workload.rustraft_build_profile.is_empty()
            && workload.rustraft_build_profile != "release"
        {
            blockers.push(format!(
                "benchmark:summary_rustraft_build_profile_not_release:{}:{}",
                workload.workload.id(),
                workload.rustraft_build_profile
            ));
        }
        if workload.reference_raft_harness_kind
            != RustRaftBenchmarkHarnessKind::FullReferenceRaftHarness
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_full_harness_missing:{}:{}",
                workload.workload.id(),
                workload.reference_raft_harness_kind.id()
            ));
        }
        if workload.rustraft_harness_kind != RustRaftBenchmarkHarnessKind::RustRaftRuntime {
            blockers.push(format!(
                "benchmark:summary_rustraft_runtime_harness_missing:{}:{}",
                workload.workload.id(),
                workload.rustraft_harness_kind.id()
            ));
        }
        if workload.node_count != summary.options.node_count {
            blockers.push(format!(
                "benchmark:summary_sample_node_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.node_count,
                summary.options.node_count
            ));
        }
        if workload.reference_raft_node_count != summary.options.node_count {
            blockers.push(format!(
                "benchmark:summary_reference_raft_node_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_node_count,
                summary.options.node_count
            ));
        }
        if workload.rustraft_node_count != summary.options.node_count {
            blockers.push(format!(
                "benchmark:summary_rustraft_node_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_node_count,
                summary.options.node_count
            ));
        }
        if workload.reference_raft_iterations_per_workload
            != summary.options.iterations_per_workload
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_iterations_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_iterations_per_workload,
                summary.options.iterations_per_workload
            ));
        }
        if workload.rustraft_iterations_per_workload != summary.options.iterations_per_workload {
            blockers.push(format!(
                "benchmark:summary_rustraft_iterations_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_iterations_per_workload,
                summary.options.iterations_per_workload
            ));
        }
        if workload.reference_raft_batch_size != summary.options.batch_size {
            blockers.push(format!(
                "benchmark:summary_reference_raft_batch_size_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_batch_size,
                summary.options.batch_size
            ));
        }
        if workload.rustraft_batch_size != summary.options.batch_size {
            blockers.push(format!(
                "benchmark:summary_rustraft_batch_size_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_batch_size,
                summary.options.batch_size
            ));
        }
        if workload.reference_raft_payload_size_bytes != summary.options.payload_size_bytes {
            blockers.push(format!(
                "benchmark:summary_reference_raft_payload_size_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_payload_size_bytes,
                summary.options.payload_size_bytes
            ));
        }
        if workload.rustraft_payload_size_bytes != summary.options.payload_size_bytes {
            blockers.push(format!(
                "benchmark:summary_rustraft_payload_size_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_payload_size_bytes,
                summary.options.payload_size_bytes
            ));
        }
        if workload.reference_raft_timed_iteration_count != summary.options.iterations_per_workload
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_timed_iteration_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_timed_iteration_count,
                summary.options.iterations_per_workload
            ));
        }
        if workload.rustraft_timed_iteration_count != summary.options.iterations_per_workload {
            blockers.push(format!(
                "benchmark:summary_rustraft_timed_iteration_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_timed_iteration_count,
                summary.options.iterations_per_workload
            ));
        }
        let expected_operations_per_timed_iteration =
            writes_per_iteration(workload.workload, &summary.options);
        if workload.reference_raft_operations_per_timed_iteration
            != expected_operations_per_timed_iteration
        {
            blockers.push(format!(
                "benchmark:summary_reference_raft_operations_per_timed_iteration_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_operations_per_timed_iteration,
                expected_operations_per_timed_iteration
            ));
        }
        if workload.rustraft_operations_per_timed_iteration
            != expected_operations_per_timed_iteration
        {
            blockers.push(format!(
                "benchmark:summary_rustraft_operations_per_timed_iteration_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_operations_per_timed_iteration,
                expected_operations_per_timed_iteration
            ));
        }
        if workload.reference_raft_total_duration_micros == 0 {
            blockers.push(format!(
                "benchmark:summary_reference_raft_total_duration_zero:{}",
                workload.workload.id()
            ));
        }
        if workload.rustraft_total_duration_micros == 0 {
            blockers.push(format!(
                "benchmark:summary_rustraft_total_duration_zero:{}",
                workload.workload.id()
            ));
        }
        let expected_operation_count = operation_count_for(workload.workload, &summary.options);
        if workload.reference_raft_operation_count != expected_operation_count {
            blockers.push(format!(
                "benchmark:summary_reference_raft_operation_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.reference_raft_operation_count,
                expected_operation_count
            ));
        }
        if workload.rustraft_operation_count != expected_operation_count {
            blockers.push(format!(
                "benchmark:summary_rustraft_operation_count_mismatch:{}:{}:{}",
                workload.workload.id(),
                workload.rustraft_operation_count,
                expected_operation_count
            ));
        }
        if workload.reference_raft_operation_count == 0 || workload.rustraft_operation_count == 0 {
            blockers.push(format!(
                "benchmark:summary_zero_operations:{}",
                workload.workload.id()
            ));
        }
        blockers.extend(summary_sample_metric_blockers(
            "reference_raft",
            workload.workload,
            workload.reference_raft_p50_latency_micros,
            workload.reference_raft_p99_latency_micros,
            workload.reference_raft_throughput_ops_per_sec,
            workload.reference_raft_operation_count,
            workload.reference_raft_timed_iteration_count,
            workload.reference_raft_total_duration_micros,
        ));
        blockers.extend(summary_sample_metric_blockers(
            "rustraft",
            workload.workload,
            workload.rustraft_p50_latency_micros,
            workload.rustraft_p99_latency_micros,
            workload.rustraft_throughput_ops_per_sec,
            workload.rustraft_operation_count,
            workload.rustraft_timed_iteration_count,
            workload.rustraft_total_duration_micros,
        ));
        push_workload_summary_ratio_finite_blocker(
            &mut blockers,
            workload.workload,
            "p50",
            workload.p50_ratio,
            ratio(
                workload.rustraft_p50_latency_micros as f64,
                workload.reference_raft_p50_latency_micros as f64,
            ),
        );
        push_workload_summary_ratio_finite_blocker(
            &mut blockers,
            workload.workload,
            "p99",
            workload.p99_ratio,
            ratio(
                workload.rustraft_p99_latency_micros as f64,
                workload.reference_raft_p99_latency_micros as f64,
            ),
        );
        push_workload_summary_ratio_finite_blocker(
            &mut blockers,
            workload.workload,
            "throughput",
            workload.throughput_ratio,
            ratio(
                workload.rustraft_throughput_ops_per_sec,
                workload.reference_raft_throughput_ops_per_sec,
            ),
        );
        push_workload_summary_ratio_mismatch(
            &mut blockers,
            workload.workload,
            "p50",
            workload.p50_ratio,
            ratio(
                workload.rustraft_p50_latency_micros as f64,
                workload.reference_raft_p50_latency_micros as f64,
            ),
        );
        push_workload_summary_ratio_mismatch(
            &mut blockers,
            workload.workload,
            "p99",
            workload.p99_ratio,
            ratio(
                workload.rustraft_p99_latency_micros as f64,
                workload.reference_raft_p99_latency_micros as f64,
            ),
        );
        push_workload_summary_ratio_mismatch(
            &mut blockers,
            workload.workload,
            "throughput",
            workload.throughput_ratio,
            ratio(
                workload.rustraft_throughput_ops_per_sec,
                workload.reference_raft_throughput_ops_per_sec,
            ),
        );
        if !workload.p50_ratio.is_finite() || workload.p50_ratio > max_latency_ratio {
            blockers.push(format!(
                "benchmark:summary_p50_regression:{}:{:.6}:{:.6}",
                workload.workload.id(),
                workload.p50_ratio,
                max_latency_ratio
            ));
        }
        if !workload.p99_ratio.is_finite() || workload.p99_ratio > max_latency_ratio {
            blockers.push(format!(
                "benchmark:summary_p99_regression:{}:{:.6}:{:.6}",
                workload.workload.id(),
                workload.p99_ratio,
                max_latency_ratio
            ));
        }
        if !workload.throughput_ratio.is_finite()
            || workload.throughput_ratio < min_throughput_ratio
        {
            blockers.push(format!(
                "benchmark:summary_throughput_regression:{}:{:.6}:{:.6}",
                workload.workload.id(),
                workload.throughput_ratio,
                min_throughput_ratio
            ));
        }
        blockers.extend(
            workload
                .blockers
                .iter()
                .map(|blocker| format!("{}:{blocker}", workload.workload.id())),
        );
    }
    blockers.extend(summary.blockers.iter().cloned());
    if blockers.is_empty() {
        Ok(())
    } else {
        blockers.sort();
        blockers.dedup();
        Err(blockers.join("; "))
    }
}

pub fn rustraft_reference_raft_benchmark_evidence_from_artifacts(
    report: &RustRaftBenchmarkReport,
    summary: &RustRaftBenchmarkFailureSummary,
) -> Result<RustRaftReferenceRaftBenchmarkEvidence, String> {
    let expected = rustraft_reference_raft_benchmark_failure_summary(report);
    if summary != &expected {
        return Err("benchmark:summary_artifact_mismatch".to_string());
    }
    Ok(rustraft_reference_raft_benchmark_evidence(report))
}

pub fn rustraft_reference_raft_benchmark_evidence_from_summary(
    summary: &RustRaftBenchmarkFailureSummary,
) -> RustRaftReferenceRaftBenchmarkEvidence {
    let has_required_workloads = benchmark_summary_has_required_workload_set(summary);
    RustRaftReferenceRaftBenchmarkEvidence {
        real_reference_raft: has_required_workloads
            && summary.workloads.iter().all(|workload| {
                workload.reference_raft_engine_source
                    == RustRaftBenchmarkEngineSource::RealReferenceRaft
            }),
        rustraft_runtime: has_required_workloads
            && summary.workloads.iter().all(|workload| {
                workload.rustraft_engine_source == RustRaftBenchmarkEngineSource::RustRaftRuntime
            }),
        reference_raft_cpp_reference: has_required_workloads
            && summary.workloads.iter().all(|workload| {
                workload.reference_raft_implementation
                    == RustRaftBenchmarkImplementation::ReferenceRaftCpp
            }),
        rustraft_rust_candidate: has_required_workloads
            && summary.workloads.iter().all(|workload| {
                workload.rustraft_implementation == RustRaftBenchmarkImplementation::RustRaftRust
            }),
        correctness_passed: has_required_workloads
            && summary.correctness_blocker_count == 0
            && summary.workloads.iter().all(|workload| {
                workload.reference_raft_correctness_passed && workload.rustraft_correctness_passed
            }),
        performance_within_threshold: has_required_workloads
            && summary.performance_blocker_count == 0
            && summary.workloads.iter().all(|workload| workload.passed),
        workloads: summary
            .workloads
            .iter()
            .map(|workload| workload.workload.id().to_string())
            .collect(),
        blockers: rustraft_reference_raft_summary_blockers(summary),
        missing_reference_raft_binaries: summary_blockers_with_prefix(
            summary,
            "benchmark:summary_missing_reference_raft",
        ),
        unsupported_workloads: summary_blockers_with_prefix(
            summary,
            "benchmark:summary_unsupported",
        ),
        correctness_blockers: summary_blockers_with_prefix(
            summary,
            "benchmark:summary_correctness",
        ),
        performance_blockers: summary_blockers_with_prefix(
            summary,
            "benchmark:summary_performance",
        ),
    }
}

fn rustraft_reference_raft_summary_blockers(
    summary: &RustRaftBenchmarkFailureSummary,
) -> Vec<String> {
    match rustraft_assert_production_reference_raft_summary(summary) {
        Ok(()) => Vec::new(),
        Err(error) => error
            .split("; ")
            .filter(|blocker| !blocker.is_empty())
            .map(str::to_string)
            .collect(),
    }
}

fn summary_blockers_with_prefix(
    summary: &RustRaftBenchmarkFailureSummary,
    prefix: &str,
) -> Vec<String> {
    rustraft_reference_raft_summary_blockers(summary)
        .into_iter()
        .filter(|blocker| blocker.starts_with(prefix))
        .collect()
}

pub fn rustraft_reference_raft_benchmark_failure_summary(
    report: &RustRaftBenchmarkReport,
) -> RustRaftBenchmarkFailureSummary {
    let evidence = rustraft_reference_raft_benchmark_evidence(report);
    let classified = evidence
        .missing_reference_raft_binaries
        .iter()
        .chain(evidence.unsupported_workloads.iter())
        .chain(evidence.correctness_blockers.iter())
        .chain(evidence.performance_blockers.iter())
        .collect::<std::collections::BTreeSet<_>>();
    let uncategorized_blocker_count = evidence
        .blockers
        .iter()
        .filter(|blocker| !classified.contains(blocker))
        .count();
    let worst_p50_ratio = report
        .comparisons
        .iter()
        .map(|comparison| comparison.p50_ratio)
        .fold(0.0, f64::max);
    let worst_p99_ratio = report
        .comparisons
        .iter()
        .map(|comparison| comparison.p99_ratio)
        .fold(0.0, f64::max);
    let worst_throughput_ratio = report
        .comparisons
        .iter()
        .map(|comparison| comparison.throughput_ratio)
        .fold(f64::INFINITY, f64::min);
    let worst_throughput_ratio = if worst_throughput_ratio.is_finite() {
        worst_throughput_ratio
    } else {
        0.0
    };
    RustRaftBenchmarkFailureSummary {
        schema: RUSTRAFT_BENCHMARK_SUMMARY_SCHEMA.to_string(),
        generated_at_unix_ms: report.generated_at_unix_ms,
        benchmark_run_id: report.benchmark_run_id.clone(),
        environment_fingerprint: report.environment_fingerprint.clone(),
        passed: report.passed,
        production_evidence_ready: evidence.real_reference_raft
            && evidence.rustraft_runtime
            && evidence.reference_raft_cpp_reference
            && evidence.rustraft_rust_candidate
            && evidence.correctness_passed
            && evidence.performance_within_threshold
            && evidence.blockers.is_empty(),
        options: report.options.clone(),
        required_workloads: report.required_workloads.clone(),
        workload_count: report.comparisons.len(),
        failed_workload_count: report
            .comparisons
            .iter()
            .filter(|comparison| !comparison.passed)
            .count(),
        workloads: report
            .comparisons
            .iter()
            .map(rustraft_reference_raft_workload_summary)
            .collect(),
        missing_reference_raft_binary_count: evidence.missing_reference_raft_binaries.len(),
        unsupported_workload_count: evidence.unsupported_workloads.len(),
        correctness_blocker_count: evidence.correctness_blockers.len(),
        performance_blocker_count: evidence.performance_blockers.len(),
        uncategorized_blocker_count,
        worst_p50_ratio,
        worst_p99_ratio,
        worst_throughput_ratio,
        blockers: evidence.blockers,
    }
}

fn rustraft_reference_raft_workload_summary(
    comparison: &RustRaftBenchmarkComparison,
) -> RustRaftBenchmarkWorkloadSummary {
    RustRaftBenchmarkWorkloadSummary {
        workload: comparison.workload,
        passed: comparison.passed,
        reference_raft_correctness_passed: comparison.reference_raft.correctness_passed,
        rustraft_correctness_passed: comparison.rustraft.correctness_passed,
        reference_raft_engine_source: comparison.reference_raft.engine_source,
        rustraft_engine_source: comparison.rustraft.engine_source,
        reference_raft_benchmark_run_id: comparison.reference_raft.benchmark_run_id.clone(),
        rustraft_benchmark_run_id: comparison.rustraft.benchmark_run_id.clone(),
        reference_raft_implementation: comparison.reference_raft.implementation,
        rustraft_implementation: comparison.rustraft.implementation,
        reference_raft_binary_path: comparison.reference_raft.binary_path.clone(),
        rustraft_binary_path: comparison.rustraft.binary_path.clone(),
        reference_raft_git_revision: comparison.reference_raft.git_revision.clone(),
        rustraft_git_revision: comparison.rustraft.git_revision.clone(),
        reference_raft_build_profile: comparison.reference_raft.build_profile.clone(),
        rustraft_build_profile: comparison.rustraft.build_profile.clone(),
        reference_raft_harness_kind: comparison.reference_raft.harness_kind,
        rustraft_harness_kind: comparison.rustraft.harness_kind,
        node_count: comparison.reference_raft.node_count,
        reference_raft_node_count: comparison.reference_raft.node_count,
        rustraft_node_count: comparison.rustraft.node_count,
        reference_raft_iterations_per_workload: comparison.reference_raft.iterations_per_workload,
        rustraft_iterations_per_workload: comparison.rustraft.iterations_per_workload,
        reference_raft_batch_size: comparison.reference_raft.batch_size,
        rustraft_batch_size: comparison.rustraft.batch_size,
        reference_raft_payload_size_bytes: comparison.reference_raft.payload_size_bytes,
        rustraft_payload_size_bytes: comparison.rustraft.payload_size_bytes,
        reference_raft_timed_iteration_count: comparison.reference_raft.timed_iteration_count,
        rustraft_timed_iteration_count: comparison.rustraft.timed_iteration_count,
        reference_raft_operations_per_timed_iteration: comparison
            .reference_raft
            .operations_per_timed_iteration,
        rustraft_operations_per_timed_iteration: comparison.rustraft.operations_per_timed_iteration,
        reference_raft_total_duration_micros: comparison.reference_raft.total_duration_micros,
        rustraft_total_duration_micros: comparison.rustraft.total_duration_micros,
        reference_raft_operation_count: comparison.reference_raft.operation_count,
        rustraft_operation_count: comparison.rustraft.operation_count,
        reference_raft_p50_latency_micros: comparison.reference_raft.p50_latency_micros,
        rustraft_p50_latency_micros: comparison.rustraft.p50_latency_micros,
        reference_raft_p99_latency_micros: comparison.reference_raft.p99_latency_micros,
        rustraft_p99_latency_micros: comparison.rustraft.p99_latency_micros,
        reference_raft_throughput_ops_per_sec: comparison.reference_raft.throughput_ops_per_sec,
        rustraft_throughput_ops_per_sec: comparison.rustraft.throughput_ops_per_sec,
        p50_ratio: comparison.p50_ratio,
        p99_ratio: comparison.p99_ratio,
        throughput_ratio: comparison.throughput_ratio,
        blockers: comparison
            .blockers
            .iter()
            .map(|blocker| benchmark_blocker_id(blocker))
            .collect(),
    }
}

pub fn rustraft_reference_raft_benchmark_evidence(
    report: &RustRaftBenchmarkReport,
) -> RustRaftReferenceRaftBenchmarkEvidence {
    let mut blockers = Vec::new();
    let mut workloads = Vec::new();
    let mut missing_reference_raft_binaries = Vec::new();
    let mut unsupported_workloads = Vec::new();
    let mut correctness_blockers = Vec::new();
    let mut performance_blockers = Vec::new();
    let has_required_workloads = benchmark_report_has_required_workload_set(report);
    let real_reference_raft = has_required_workloads
        && report.comparisons.iter().all(|comparison| {
            comparison.reference_raft.engine_source
                == RustRaftBenchmarkEngineSource::RealReferenceRaft
        });
    let rustraft_runtime = has_required_workloads
        && report.comparisons.iter().all(|comparison| {
            comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::RustRaftRuntime
        });
    let reference_raft_cpp_reference = has_required_workloads
        && report.comparisons.iter().all(|comparison| {
            comparison.reference_raft.implementation
                == RustRaftBenchmarkImplementation::ReferenceRaftCpp
        });
    let rustraft_rust_candidate = has_required_workloads
        && report.comparisons.iter().all(|comparison| {
            comparison.rustraft.implementation == RustRaftBenchmarkImplementation::RustRaftRust
        });
    let option_blockers = benchmark_options_blockers(report);
    let correctness_passed = has_required_workloads
        && report.comparisons.iter().all(|comparison| {
            comparison.reference_raft.correctness_passed && comparison.rustraft.correctness_passed
        })
        && option_blockers.is_empty();
    let performance_within_threshold = has_required_workloads
        && report
            .comparisons
            .iter()
            .all(|comparison| comparison.passed);
    for blocker in option_blockers {
        classify_benchmark_blocker(
            &blocker,
            &mut missing_reference_raft_binaries,
            &mut unsupported_workloads,
            &mut correctness_blockers,
            &mut performance_blockers,
        );
        blockers.push(blocker);
    }
    for comparison in &report.comparisons {
        workloads.push(comparison.workload.id().to_string());
        if comparison.reference_raft.engine_source == RustRaftBenchmarkEngineSource::Model {
            blockers.push(format!(
                "benchmark:model_reference_raft:{}",
                comparison.workload.id()
            ));
        }
        if comparison.rustraft.engine_source == RustRaftBenchmarkEngineSource::Model {
            blockers.push(format!(
                "benchmark:model_rustraft:{}",
                comparison.workload.id()
            ));
        }
        if comparison.reference_raft.implementation
            != RustRaftBenchmarkImplementation::ReferenceRaftCpp
        {
            let blocker = format!(
                "{}:benchmark:reference_raft_implementation_mismatch:{}:reference_raft_cpp",
                comparison.workload.id(),
                comparison.reference_raft.implementation.id()
            );
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        if comparison.rustraft.implementation != RustRaftBenchmarkImplementation::RustRaftRust {
            let blocker = format!(
                "{}:benchmark:rustraft_implementation_mismatch:{}:rustraft_rust",
                comparison.workload.id(),
                comparison.rustraft.implementation.id()
            );
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in
            benchmark_sample_provenance_blockers("reference_raft", &comparison.reference_raft)
        {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in benchmark_sample_shape_blockers(
            "reference_raft",
            &comparison.reference_raft,
            comparison.workload,
            &report.options,
        ) {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in benchmark_sample_provenance_blockers("rustraft", &comparison.rustraft) {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in benchmark_sample_pair_provenance_blockers(
            &comparison.reference_raft,
            &comparison.rustraft,
        ) {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in benchmark_sample_shape_blockers(
            "rustraft",
            &comparison.rustraft,
            comparison.workload,
            &report.options,
        ) {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in benchmark_comparison_integrity_blockers(comparison, &report.options) {
            let blocker = format!("{}:{blocker}", comparison.workload.id());
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
        for blocker in &comparison.blockers {
            let blocker = benchmark_blocker_id(blocker);
            let blocker = format!("{}:{}", comparison.workload.id(), blocker);
            classify_benchmark_blocker(
                &blocker,
                &mut missing_reference_raft_binaries,
                &mut unsupported_workloads,
                &mut correctness_blockers,
                &mut performance_blockers,
            );
            blockers.push(blocker);
        }
    }
    if !real_reference_raft {
        let blocker = "benchmark:real_reference_raft_missing".to_string();
        classify_benchmark_blocker(
            &blocker,
            &mut missing_reference_raft_binaries,
            &mut unsupported_workloads,
            &mut correctness_blockers,
            &mut performance_blockers,
        );
        blockers.push(blocker);
    }
    if !rustraft_runtime {
        blockers.push("benchmark:rustraft_runtime_missing".to_string());
    }
    RustRaftReferenceRaftBenchmarkEvidence {
        real_reference_raft,
        rustraft_runtime,
        reference_raft_cpp_reference,
        rustraft_rust_candidate,
        correctness_passed,
        performance_within_threshold,
        workloads,
        blockers,
        missing_reference_raft_binaries,
        unsupported_workloads,
        correctness_blockers,
        performance_blockers,
    }
}

fn benchmark_report_has_required_workload_set(report: &RustRaftBenchmarkReport) -> bool {
    let required_workloads = rustraft_reference_raft_benchmark_required_workloads()
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let observed_workloads = report
        .comparisons
        .iter()
        .map(|comparison| comparison.workload.id().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    report.comparisons.len() == required_workloads.len() && observed_workloads == required_workloads
}

fn benchmark_summary_has_required_workload_set(summary: &RustRaftBenchmarkFailureSummary) -> bool {
    let required_workloads = rustraft_reference_raft_benchmark_required_workloads()
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let observed_workloads = summary
        .workloads
        .iter()
        .map(|workload| workload.workload.id().to_string())
        .collect::<std::collections::BTreeSet<_>>();
    summary.workloads.len() == required_workloads.len() && observed_workloads == required_workloads
}

fn benchmark_sample_provenance_blockers(
    engine: &str,
    sample: &RustRaftBenchmarkSample,
) -> Vec<String> {
    if sample.engine_source == RustRaftBenchmarkEngineSource::Model {
        return Vec::new();
    }
    let mut blockers = Vec::new();
    blockers.extend(benchmark_binary_path_blockers(
        engine,
        sample.binary_path.as_deref(),
    ));
    blockers.extend(benchmark_git_revision_blockers(
        engine,
        sample.git_revision.as_deref(),
    ));
    if sample.build_profile.is_empty() {
        blockers.push(format!(
            "benchmark:{engine}_provenance_build_profile_missing"
        ));
    }
    if engine == "reference_raft"
        && sample.harness_kind != RustRaftBenchmarkHarnessKind::FullReferenceRaftHarness
    {
        blockers.push(format!(
            "benchmark:reference_raft_full_harness_missing:{}",
            sample.harness_kind.id()
        ));
    }
    if engine == "rustraft" && sample.harness_kind != RustRaftBenchmarkHarnessKind::RustRaftRuntime
    {
        blockers.push(format!(
            "benchmark:rustraft_runtime_harness_missing:{}",
            sample.harness_kind.id()
        ));
    }
    blockers
}

fn benchmark_git_revision_blockers(engine: &str, git_revision: Option<&str>) -> Vec<String> {
    let mut blockers = Vec::new();
    let Some(git_revision) = git_revision.filter(|revision| !revision.is_empty()) else {
        blockers.push(format!(
            "benchmark:{engine}_provenance_git_revision_missing"
        ));
        return blockers;
    };
    if !benchmark_git_revision_shape_valid(git_revision) {
        blockers.push(format!(
            "benchmark:{engine}_provenance_git_revision_invalid:{git_revision}"
        ));
    }
    blockers
}

fn summary_git_revision_blockers(
    engine: &str,
    workload: RustRaftBenchmarkWorkload,
    git_revision: Option<&str>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let workload_id = workload.id();
    let Some(git_revision) = git_revision.filter(|revision| !revision.is_empty()) else {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_git_revision_missing:{workload_id}"
        ));
        return blockers;
    };
    if !benchmark_git_revision_shape_valid(git_revision) {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_git_revision_invalid:{workload_id}:{git_revision}"
        ));
    }
    blockers
}

fn benchmark_git_revision_shape_valid(git_revision: &str) -> bool {
    (7..=40).contains(&git_revision.len())
        && git_revision.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn benchmark_binary_path_blockers(engine: &str, binary_path: Option<&str>) -> Vec<String> {
    let mut blockers = Vec::new();
    let Some(binary_path) = binary_path.filter(|path| !path.is_empty()) else {
        blockers.push(format!("benchmark:{engine}_provenance_binary_path_missing"));
        return blockers;
    };
    let path = Path::new(binary_path);
    if !path.is_absolute() {
        blockers.push(format!(
            "benchmark:{engine}_provenance_binary_path_not_absolute:{binary_path}"
        ));
    }
    if !path.is_file() {
        blockers.push(format!(
            "benchmark:{engine}_provenance_binary_path_not_file:{binary_path}"
        ));
    } else if !benchmark_binary_path_is_executable(path) {
        blockers.push(format!(
            "benchmark:{engine}_provenance_binary_path_not_executable:{binary_path}"
        ));
    }
    blockers
}

fn summary_binary_path_blockers(
    engine: &str,
    workload: RustRaftBenchmarkWorkload,
    binary_path: Option<&str>,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let workload_id = workload.id();
    let Some(binary_path) = binary_path.filter(|path| !path.is_empty()) else {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_binary_path_missing:{workload_id}"
        ));
        return blockers;
    };
    let path = Path::new(binary_path);
    if !path.is_absolute() {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_binary_path_not_absolute:{workload_id}:{binary_path}"
        ));
    }
    if !path.is_file() {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_binary_path_not_file:{workload_id}:{binary_path}"
        ));
    } else if !benchmark_binary_path_is_executable(path) {
        blockers.push(format!(
            "benchmark:summary_{engine}_provenance_binary_path_not_executable:{workload_id}:{binary_path}"
        ));
    }
    blockers
}

#[cfg(unix)]
fn benchmark_binary_path_is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;

    fs::metadata(path)
        .map(|metadata| metadata.permissions().mode() & 0o111 != 0)
        .unwrap_or(false)
}

#[cfg(not(unix))]
fn benchmark_binary_path_is_executable(_path: &Path) -> bool {
    true
}

fn benchmark_sample_pair_provenance_blockers(
    reference_raft: &RustRaftBenchmarkSample,
    rustraft: &RustRaftBenchmarkSample,
) -> Vec<String> {
    let mut blockers = Vec::new();
    if !reference_raft.build_profile.is_empty()
        && !rustraft.build_profile.is_empty()
        && reference_raft.build_profile != rustraft.build_profile
    {
        blockers.push(format!(
            "benchmark:build_profile_mismatch:{}:{}",
            reference_raft.build_profile, rustraft.build_profile
        ));
    }
    if let (Some(reference_raft_binary_path), Some(rustraft_binary_path)) = (
        reference_raft.binary_path.as_deref(),
        rustraft.binary_path.as_deref(),
    ) {
        if !reference_raft_binary_path.is_empty()
            && reference_raft_binary_path == rustraft_binary_path
        {
            blockers.push(format!(
                "benchmark:binary_path_collision:{reference_raft_binary_path}"
            ));
        }
    }
    if !reference_raft.build_profile.is_empty() && reference_raft.build_profile != "release" {
        blockers.push(format!(
            "benchmark:reference_raft_build_profile_not_release:{}",
            reference_raft.build_profile
        ));
    }
    if !rustraft.build_profile.is_empty() && rustraft.build_profile != "release" {
        blockers.push(format!(
            "benchmark:rustraft_build_profile_not_release:{}",
            rustraft.build_profile
        ));
    }
    blockers
}

fn benchmark_sample_shape_blockers(
    engine: &str,
    sample: &RustRaftBenchmarkSample,
    expected_workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let expected_engine = match engine {
        "reference_raft" => Some(RustRaftBenchmarkEngine::ReferenceRaft),
        "rustraft" => Some(RustRaftBenchmarkEngine::RustRaft),
        _ => None,
    };
    let expected_engine_source = match engine {
        "reference_raft" => Some(RustRaftBenchmarkEngineSource::RealReferenceRaft),
        "rustraft" => Some(RustRaftBenchmarkEngineSource::RustRaftRuntime),
        _ => None,
    };
    if let Some(expected_engine) = expected_engine {
        if sample.engine != expected_engine {
            blockers.push(format!(
                "benchmark:{engine}_sample_engine_mismatch:{:?}:{:?}",
                sample.engine, expected_engine
            ));
        }
    }
    if let Some(expected_engine_source) = expected_engine_source {
        if sample.engine_source != expected_engine_source {
            blockers.push(format!(
                "benchmark:{engine}_sample_engine_source_mismatch:{:?}:{:?}",
                sample.engine_source, expected_engine_source
            ));
        }
    }
    if sample.workload != expected_workload {
        blockers.push(format!(
            "benchmark:{engine}_sample_workload_mismatch:{}:{}",
            sample.workload.id(),
            expected_workload.id()
        ));
    }
    if sample.node_count != options.node_count {
        blockers.push(format!(
            "benchmark:{engine}_sample_node_count_mismatch:{}:{}",
            sample.node_count, options.node_count
        ));
    }
    if sample.iterations_per_workload != options.iterations_per_workload {
        blockers.push(format!(
            "benchmark:{engine}_sample_iterations_mismatch:{}:{}",
            sample.iterations_per_workload, options.iterations_per_workload
        ));
    }
    if sample.batch_size != options.batch_size {
        blockers.push(format!(
            "benchmark:{engine}_sample_batch_size_mismatch:{}:{}",
            sample.batch_size, options.batch_size
        ));
    }
    if sample.payload_size_bytes != options.payload_size_bytes {
        blockers.push(format!(
            "benchmark:{engine}_sample_payload_size_mismatch:{}:{}",
            sample.payload_size_bytes, options.payload_size_bytes
        ));
    }
    let expected_operation_count = operation_count_for(expected_workload, options);
    if sample.operation_count != expected_operation_count {
        blockers.push(format!(
            "benchmark:{engine}_sample_operation_count_mismatch:{}:{}",
            sample.operation_count, expected_operation_count
        ));
    }
    if sample.timed_iteration_count != options.iterations_per_workload {
        blockers.push(format!(
            "benchmark:{engine}_sample_timed_iteration_count_mismatch:{}:{}",
            sample.timed_iteration_count, options.iterations_per_workload
        ));
    }
    let expected_operations_per_timed_iteration = writes_per_iteration(expected_workload, options);
    if sample.operations_per_timed_iteration != expected_operations_per_timed_iteration {
        blockers.push(format!(
            "benchmark:{engine}_sample_operations_per_timed_iteration_mismatch:{}:{}",
            sample.operations_per_timed_iteration, expected_operations_per_timed_iteration
        ));
    }
    if sample.total_duration_micros == 0 {
        blockers.push(format!("benchmark:{engine}_sample_total_duration_zero"));
    } else if sample.total_duration_micros < sample.timed_iteration_count as u64 {
        blockers.push(format!(
            "benchmark:{engine}_sample_total_duration_below_timed_iterations:{}:{}",
            sample.total_duration_micros, sample.timed_iteration_count
        ));
    }
    if sample.p50_latency_micros == 0 {
        blockers.push(format!("benchmark:{engine}_sample_p50_latency_zero"));
    }
    if sample.p99_latency_micros == 0 {
        blockers.push(format!("benchmark:{engine}_sample_p99_latency_zero"));
    }
    if sample.p50_latency_micros > 0
        && sample.p99_latency_micros > 0
        && sample.p99_latency_micros < sample.p50_latency_micros
    {
        blockers.push(format!(
            "benchmark:{engine}_sample_latency_order_invalid:{}:{}",
            sample.p50_latency_micros, sample.p99_latency_micros
        ));
    }
    if sample.p50_latency_micros > sample.total_duration_micros {
        blockers.push(format!(
            "benchmark:{engine}_sample_p50_exceeds_total_duration:{}:{}",
            sample.p50_latency_micros, sample.total_duration_micros
        ));
    }
    if sample.p99_latency_micros > sample.total_duration_micros {
        blockers.push(format!(
            "benchmark:{engine}_sample_p99_exceeds_total_duration:{}:{}",
            sample.p99_latency_micros, sample.total_duration_micros
        ));
    }
    if !sample.throughput_ops_per_sec.is_finite() || sample.throughput_ops_per_sec <= 0.0 {
        blockers.push(format!("benchmark:{engine}_sample_throughput_invalid"));
    } else if sample.total_duration_micros > 0
        && !throughput_matches_duration(
            sample.operation_count,
            sample.total_duration_micros,
            sample.throughput_ops_per_sec,
        )
    {
        blockers.push(format!(
            "benchmark:{engine}_sample_throughput_duration_mismatch:{:.6}:{:.6}",
            sample.throughput_ops_per_sec,
            throughput_from_duration(sample.operation_count, sample.total_duration_micros)
        ));
    }
    blockers
}

fn benchmark_comparison_integrity_blockers(
    comparison: &RustRaftBenchmarkComparison,
    options: &RustRaftBenchmarkOptions,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let expected_p50 = ratio(
        comparison.rustraft.p50_latency_micros as f64,
        comparison.reference_raft.p50_latency_micros as f64,
    );
    let expected_p99 = ratio(
        comparison.rustraft.p99_latency_micros as f64,
        comparison.reference_raft.p99_latency_micros as f64,
    );
    let expected_throughput = ratio(
        comparison.rustraft.throughput_ops_per_sec,
        comparison.reference_raft.throughput_ops_per_sec,
    );
    push_ratio_finite_blocker(&mut blockers, "p50", comparison.p50_ratio, expected_p50);
    push_ratio_finite_blocker(&mut blockers, "p99", comparison.p99_ratio, expected_p99);
    push_ratio_finite_blocker(
        &mut blockers,
        "throughput",
        comparison.throughput_ratio,
        expected_throughput,
    );
    push_ratio_mismatch(&mut blockers, "p50", comparison.p50_ratio, expected_p50);
    push_ratio_mismatch(&mut blockers, "p99", comparison.p99_ratio, expected_p99);
    push_ratio_mismatch(
        &mut blockers,
        "throughput",
        comparison.throughput_ratio,
        expected_throughput,
    );

    let max_latency_ratio = 1.0 + options.pass_tolerance_percent / 100.0;
    let min_throughput_ratio = 1.0 - options.pass_tolerance_percent / 100.0;
    let recomputed_performance_passed = expected_p50.is_finite()
        && expected_p99.is_finite()
        && expected_throughput.is_finite()
        && expected_p50 <= max_latency_ratio
        && expected_p99 <= max_latency_ratio
        && expected_throughput >= min_throughput_ratio;
    if expected_p50 > max_latency_ratio
        && !comparison
            .blockers
            .iter()
            .any(|blocker| blocker.contains("p50_ratio"))
    {
        blockers.push(format!(
            "benchmark:comparison_missing_p50_regression_blocker:{expected_p50:.6}:{max_latency_ratio:.6}"
        ));
    }
    if expected_p99 > max_latency_ratio
        && !comparison
            .blockers
            .iter()
            .any(|blocker| blocker.contains("p99_ratio"))
    {
        blockers.push(format!(
            "benchmark:comparison_missing_p99_regression_blocker:{expected_p99:.6}:{max_latency_ratio:.6}"
        ));
    }
    if expected_throughput < min_throughput_ratio
        && !comparison
            .blockers
            .iter()
            .any(|blocker| blocker.contains("throughput_ratio"))
    {
        blockers.push(format!(
            "benchmark:comparison_missing_throughput_regression_blocker:{expected_throughput:.6}:{min_throughput_ratio:.6}"
        ));
    }
    if comparison.passed && !comparison.blockers.is_empty() {
        blockers.push("benchmark:comparison_passed_with_blockers".to_string());
    }
    if comparison.passed && !recomputed_performance_passed {
        blockers.push("benchmark:comparison_passed_despite_regression".to_string());
    }
    if !comparison.passed && comparison.blockers.is_empty() && recomputed_performance_passed {
        blockers.push("benchmark:comparison_failed_without_blockers".to_string());
    }
    blockers
}

fn push_ratio_finite_blocker(
    blockers: &mut Vec<String>,
    label: &str,
    declared: f64,
    expected: f64,
) {
    if !declared.is_finite() {
        blockers.push(format!("benchmark:comparison_{label}_ratio_not_finite"));
    }
    if !expected.is_finite() {
        blockers.push(format!(
            "benchmark:comparison_expected_{label}_ratio_not_finite"
        ));
    }
}

fn push_ratio_mismatch(blockers: &mut Vec<String>, label: &str, declared: f64, expected: f64) {
    let both_nan = declared.is_nan() && expected.is_nan();
    let both_infinite_same_sign =
        declared.is_infinite() && expected.is_infinite() && declared.signum() == expected.signum();
    let close = (declared - expected).abs() <= 0.000_001;
    if !(both_nan || both_infinite_same_sign || close) {
        blockers.push(format!(
            "benchmark:comparison_{label}_ratio_mismatch:{declared:.6}:{expected:.6}"
        ));
    }
}

fn push_summary_ratio_finite_blocker(
    blockers: &mut Vec<String>,
    label: &str,
    declared: f64,
    expected: f64,
) {
    if !declared.is_finite() {
        blockers.push(format!("benchmark:summary_{label}_ratio_not_finite"));
    }
    if !expected.is_finite() {
        blockers.push(format!(
            "benchmark:summary_expected_{label}_ratio_not_finite"
        ));
    }
}

fn push_summary_ratio_mismatch(
    blockers: &mut Vec<String>,
    label: &str,
    declared: f64,
    expected: f64,
) {
    let both_nan = declared.is_nan() && expected.is_nan();
    let both_infinite_same_sign =
        declared.is_infinite() && expected.is_infinite() && declared.signum() == expected.signum();
    let close = (declared - expected).abs() <= 0.000_001;
    if !(both_nan || both_infinite_same_sign || close) {
        blockers.push(format!(
            "benchmark:summary_{label}_ratio_mismatch:{declared:.6}:{expected:.6}"
        ));
    }
}

fn push_workload_summary_ratio_finite_blocker(
    blockers: &mut Vec<String>,
    workload: RustRaftBenchmarkWorkload,
    label: &str,
    declared: f64,
    expected: f64,
) {
    if !declared.is_finite() {
        blockers.push(format!(
            "benchmark:summary_workload_{label}_ratio_not_finite:{}",
            workload.id()
        ));
    }
    if !expected.is_finite() {
        blockers.push(format!(
            "benchmark:summary_workload_expected_{label}_ratio_not_finite:{}",
            workload.id()
        ));
    }
}

fn push_workload_summary_ratio_mismatch(
    blockers: &mut Vec<String>,
    workload: RustRaftBenchmarkWorkload,
    label: &str,
    declared: f64,
    expected: f64,
) {
    let both_nan = declared.is_nan() && expected.is_nan();
    let both_infinite_same_sign =
        declared.is_infinite() && expected.is_infinite() && declared.signum() == expected.signum();
    let close = (declared - expected).abs() <= 0.000_001;
    if !(both_nan || both_infinite_same_sign || close) {
        blockers.push(format!(
            "benchmark:summary_workload_{label}_ratio_mismatch:{}:{declared:.6}:{expected:.6}",
            workload.id()
        ));
    }
}

fn summary_sample_metric_blockers(
    engine: &str,
    workload: RustRaftBenchmarkWorkload,
    p50_latency_micros: u64,
    p99_latency_micros: u64,
    throughput_ops_per_sec: f64,
    operation_count: usize,
    timed_iteration_count: usize,
    total_duration_micros: u64,
) -> Vec<String> {
    let mut blockers = Vec::new();
    let workload_id = workload.id();
    if p50_latency_micros == 0 {
        blockers.push(format!(
            "benchmark:summary_{engine}_p50_latency_zero:{workload_id}"
        ));
    }
    if p99_latency_micros == 0 {
        blockers.push(format!(
            "benchmark:summary_{engine}_p99_latency_zero:{workload_id}"
        ));
    }
    if p50_latency_micros > 0 && p99_latency_micros > 0 && p99_latency_micros < p50_latency_micros {
        blockers.push(format!(
            "benchmark:summary_{engine}_latency_order_invalid:{workload_id}:{p50_latency_micros}:{p99_latency_micros}"
        ));
    }
    if total_duration_micros > 0 && total_duration_micros < timed_iteration_count as u64 {
        blockers.push(format!(
            "benchmark:summary_{engine}_total_duration_below_timed_iterations:{workload_id}:{total_duration_micros}:{timed_iteration_count}"
        ));
    }
    if p50_latency_micros > total_duration_micros {
        blockers.push(format!(
            "benchmark:summary_{engine}_p50_exceeds_total_duration:{workload_id}:{p50_latency_micros}:{total_duration_micros}"
        ));
    }
    if p99_latency_micros > total_duration_micros {
        blockers.push(format!(
            "benchmark:summary_{engine}_p99_exceeds_total_duration:{workload_id}:{p99_latency_micros}:{total_duration_micros}"
        ));
    }
    if !throughput_ops_per_sec.is_finite() || throughput_ops_per_sec <= 0.0 {
        blockers.push(format!(
            "benchmark:summary_{engine}_throughput_invalid:{workload_id}"
        ));
    } else if total_duration_micros > 0
        && !throughput_matches_duration(
            operation_count,
            total_duration_micros,
            throughput_ops_per_sec,
        )
    {
        blockers.push(format!(
            "benchmark:summary_{engine}_throughput_duration_mismatch:{workload_id}:{:.6}:{:.6}",
            throughput_ops_per_sec,
            throughput_from_duration(operation_count, total_duration_micros)
        ));
    }
    blockers
}

fn throughput_from_duration(operation_count: usize, total_duration_micros: u64) -> f64 {
    if total_duration_micros == 0 {
        return f64::INFINITY;
    }
    operation_count as f64 / (total_duration_micros as f64 / 1_000_000.0)
}

fn throughput_matches_duration(
    operation_count: usize,
    total_duration_micros: u64,
    declared_throughput: f64,
) -> bool {
    let expected = throughput_from_duration(operation_count, total_duration_micros);
    if !declared_throughput.is_finite() || !expected.is_finite() {
        return false;
    }
    let tolerance = expected.abs().max(1.0) * 0.000_01;
    (declared_throughput - expected).abs() <= tolerance
}

fn benchmark_now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis() as u64
}

fn benchmark_run_id() -> String {
    let sequence = RUSTRAFT_BENCHMARK_RUN_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    format!(
        "rustraft-reference-raft-parity-{}-pid-{}-seq-{}",
        benchmark_now_unix_ms(),
        process::id(),
        sequence
    )
}

fn benchmark_environment_fingerprint() -> String {
    format!(
        "os={};arch={};target={};debug_assertions={}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        option_env!("TARGET").unwrap_or("unknown"),
        cfg!(debug_assertions)
    )
}

fn benchmark_artifact_timestamp_blockers(label: &str, generated_at_unix_ms: u64) -> Vec<String> {
    if generated_at_unix_ms == 0 {
        return vec![format!("benchmark:{label}_generated_at_missing")];
    }
    let now = benchmark_now_unix_ms();
    if generated_at_unix_ms > now.saturating_add(RUSTRAFT_BENCHMARK_MAX_FUTURE_SKEW_MS) {
        return vec![format!("benchmark:{label}_generated_at_in_future")];
    }
    if now.saturating_sub(generated_at_unix_ms) > RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_MS {
        return vec![format!("benchmark:{label}_generated_at_stale")];
    }
    Vec::new()
}

fn benchmark_environment_release_blockers(
    label: &str,
    environment_fingerprint: &str,
) -> Vec<String> {
    if environment_fingerprint.is_empty() {
        return Vec::new();
    }
    if environment_fingerprint.contains("debug_assertions=true") {
        return vec![format!(
            "benchmark:{label}_environment_debug_assertions_enabled"
        )];
    }
    if !environment_fingerprint.contains("debug_assertions=false") {
        return vec![format!(
            "benchmark:{label}_environment_release_fingerprint_missing"
        )];
    }
    Vec::new()
}

fn benchmark_options_blockers(report: &RustRaftBenchmarkReport) -> Vec<String> {
    let mut blockers = Vec::new();
    if report.schema != RUSTRAFT_BENCHMARK_REPORT_SCHEMA {
        blockers.push(format!(
            "benchmark:report_schema_mismatch:{}:{}",
            report.schema, RUSTRAFT_BENCHMARK_REPORT_SCHEMA
        ));
    }
    blockers.extend(benchmark_artifact_timestamp_blockers(
        "report",
        report.generated_at_unix_ms,
    ));
    if report.benchmark_run_id.is_empty() {
        blockers.push("benchmark:report_run_id_missing".to_string());
    }
    if report.environment_fingerprint.is_empty() {
        blockers.push("benchmark:report_environment_fingerprint_missing".to_string());
    }
    blockers.extend(benchmark_environment_release_blockers(
        "report",
        &report.environment_fingerprint,
    ));
    let actual_passed = benchmark_report_has_required_workload_set(report)
        && report
            .comparisons
            .iter()
            .all(|comparison| comparison.passed);
    if report.passed != actual_passed {
        blockers.push(format!(
            "benchmark:report_passed_mismatch:declared_{}_actual_{}",
            report.passed, actual_passed
        ));
    }
    if !report.correctness_required {
        blockers.push("benchmark:correctness_required_disabled".to_string());
    }
    if report.node_count != report.options.node_count {
        blockers.push(format!(
            "benchmark:report_node_count_mismatch:{}:{}",
            report.node_count, report.options.node_count
        ));
    }
    if (report.pass_tolerance_percent - report.options.pass_tolerance_percent).abs() > f64::EPSILON
    {
        blockers.push(format!(
            "benchmark:report_pass_tolerance_mismatch:{:.3}:{:.3}",
            report.pass_tolerance_percent, report.options.pass_tolerance_percent
        ));
    }
    blockers.extend(benchmark_production_option_blockers(&report.options));
    let required_workload_manifest = rustraft_reference_raft_benchmark_required_workloads();
    if report.required_workloads != required_workload_manifest {
        blockers.push(format!(
            "benchmark:report_required_workloads_mismatch:declared_{}_required_{}",
            report.required_workloads.join(","),
            required_workload_manifest.join(",")
        ));
    }
    let observed_workload_order = report
        .comparisons
        .iter()
        .map(|comparison| comparison.workload.id().to_string())
        .collect::<Vec<_>>();
    if observed_workload_order != required_workload_manifest {
        blockers.push(format!(
            "benchmark:report_workload_order_mismatch:observed_{}_required_{}",
            observed_workload_order.join(","),
            required_workload_manifest.join(",")
        ));
    }
    let required_workloads = required_workload_manifest
        .into_iter()
        .collect::<std::collections::BTreeSet<_>>();
    let mut observed_workloads = std::collections::BTreeSet::new();
    let mut duplicate_workloads = std::collections::BTreeSet::new();
    for comparison in &report.comparisons {
        let workload_id = comparison.workload.id().to_string();
        if !observed_workloads.insert(workload_id.clone()) {
            duplicate_workloads.insert(workload_id);
        }
        if comparison.reference_raft.benchmark_run_id.is_empty() {
            blockers.push(format!(
                "benchmark:report_reference_raft_run_id_missing:{}",
                comparison.workload.id()
            ));
        } else if comparison.reference_raft.benchmark_run_id != report.benchmark_run_id {
            blockers.push(format!(
                "benchmark:report_reference_raft_run_id_mismatch:{}:{}:{}",
                comparison.workload.id(),
                comparison.reference_raft.benchmark_run_id,
                report.benchmark_run_id
            ));
        }
        if comparison.rustraft.benchmark_run_id.is_empty() {
            blockers.push(format!(
                "benchmark:report_rustraft_run_id_missing:{}",
                comparison.workload.id()
            ));
        } else if comparison.rustraft.benchmark_run_id != report.benchmark_run_id {
            blockers.push(format!(
                "benchmark:report_rustraft_run_id_mismatch:{}:{}:{}",
                comparison.workload.id(),
                comparison.rustraft.benchmark_run_id,
                report.benchmark_run_id
            ));
        }
        if !comparison.reference_raft.benchmark_run_id.is_empty()
            && !comparison.rustraft.benchmark_run_id.is_empty()
            && comparison.reference_raft.benchmark_run_id != comparison.rustraft.benchmark_run_id
        {
            blockers.push(format!(
                "benchmark:report_sample_run_id_pair_mismatch:{}:{}:{}",
                comparison.workload.id(),
                comparison.reference_raft.benchmark_run_id,
                comparison.rustraft.benchmark_run_id
            ));
        }
    }
    if report.comparisons.len() != required_workloads.len() {
        blockers.push(format!(
            "benchmark:report_required_workload_count_mismatch:declared_{}_required_{}",
            report.comparisons.len(),
            required_workloads.len()
        ));
    }
    for workload in required_workloads.difference(&observed_workloads) {
        blockers.push(format!(
            "benchmark:report_required_workload_missing:{workload}"
        ));
    }
    for workload in duplicate_workloads {
        blockers.push(format!("benchmark:report_duplicate_workload:{workload}"));
    }
    blockers
}

fn benchmark_production_option_blockers(options: &RustRaftBenchmarkOptions) -> Vec<String> {
    let mut blockers = Vec::new();
    if options.node_count < RUSTRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT {
        blockers.push(format!(
            "benchmark:node_count_below_production_scale:{}:{}",
            options.node_count, RUSTRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT
        ));
    }
    if options.iterations_per_workload == 0 {
        blockers.push("benchmark:invalid_iterations_per_workload:0".to_string());
    } else if options.iterations_per_workload
        < RUSTRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD
    {
        blockers.push(format!(
            "benchmark:iterations_per_workload_below_production_min:{}:{}",
            options.iterations_per_workload,
            RUSTRAFT_BENCHMARK_MIN_PRODUCTION_ITERATIONS_PER_WORKLOAD
        ));
    }
    if options.batch_size == 0 {
        blockers.push("benchmark:invalid_batch_size:0".to_string());
    } else if options.batch_size < RUSTRAFT_BENCHMARK_MIN_PRODUCTION_BATCH_SIZE {
        blockers.push(format!(
            "benchmark:batch_size_below_production_min:{}:{}",
            options.batch_size, RUSTRAFT_BENCHMARK_MIN_PRODUCTION_BATCH_SIZE
        ));
    }
    if options.payload_size_bytes == 0 {
        blockers.push("benchmark:invalid_payload_size_bytes:0".to_string());
    } else if options.payload_size_bytes < RUSTRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES {
        blockers.push(format!(
            "benchmark:payload_size_below_production_min:{}:{}",
            options.payload_size_bytes, RUSTRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES
        ));
    }
    if !options.pass_tolerance_percent.is_finite()
        || options.pass_tolerance_percent < 0.0
        || options.pass_tolerance_percent > RUSTRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT
    {
        blockers.push(format!(
            "benchmark:invalid_pass_tolerance_percent:{:.3}",
            options.pass_tolerance_percent
        ));
    }
    blockers
}

pub fn rustraft_validate_production_reference_raft_benchmark_options(
    options: &RustRaftBenchmarkOptions,
) -> Result<(), String> {
    let mut blockers = benchmark_production_option_blockers(options);
    blockers.sort();
    blockers.dedup();
    if blockers.is_empty() {
        Ok(())
    } else {
        Err(blockers.join("; "))
    }
}

fn benchmark_blocker_id(blocker: &str) -> String {
    if blocker.starts_with("benchmark:") {
        blocker.to_string()
    } else if blocker.starts_with("p99_ratio") {
        "benchmark:p99_regression".to_string()
    } else if blocker.starts_with("p50_ratio") {
        "benchmark:p50_regression".to_string()
    } else if blocker.starts_with("throughput_ratio") {
        "benchmark:throughput_regression".to_string()
    } else {
        format!("benchmark:{blocker}")
    }
}

fn classify_benchmark_blocker(
    blocker: &str,
    missing_reference_raft_binaries: &mut Vec<String>,
    unsupported_workloads: &mut Vec<String>,
    correctness_blockers: &mut Vec<String>,
    performance_blockers: &mut Vec<String>,
) {
    if blocker.contains("benchmark:reference_raft_kvserver_binary_missing")
        || blocker.contains("benchmark:reference_raft_kvbench_binary_missing")
        || blocker.contains("benchmark:real_reference_raft_missing")
    {
        missing_reference_raft_binaries.push(blocker.to_string());
    }
    if blocker.contains("benchmark:reference_raft_native_kvbench_unsupported")
        || blocker.contains("benchmark:workload_missing")
    {
        unsupported_workloads.push(blocker.to_string());
    }
    if blocker.contains("correctness_failed")
        || blocker.contains("benchmark:invalid_")
        || blocker.contains("benchmark:report_schema_mismatch")
        || blocker.contains("benchmark:report_run_id_missing")
        || blocker.contains("benchmark:report_reference_raft_run_id_missing")
        || blocker.contains("benchmark:report_reference_raft_run_id_mismatch")
        || blocker.contains("benchmark:report_rustraft_run_id_missing")
        || blocker.contains("benchmark:report_rustraft_run_id_mismatch")
        || blocker.contains("benchmark:report_sample_run_id_pair_mismatch")
        || blocker.contains("benchmark:report_node_count_mismatch")
        || blocker.contains("benchmark:report_environment_fingerprint_missing")
        || blocker.contains("benchmark:report_environment_debug_assertions_enabled")
        || blocker.contains("benchmark:report_environment_release_fingerprint_missing")
        || blocker.contains("benchmark:report_passed_mismatch")
        || blocker.contains("benchmark:correctness_required_disabled")
        || blocker.contains("benchmark:report_pass_tolerance_mismatch")
        || blocker.contains("benchmark:report_required_workloads_mismatch")
        || blocker.contains("benchmark:report_workload_order_mismatch")
        || blocker.contains("benchmark:node_count_below_production_scale")
        || blocker.contains("benchmark:iterations_per_workload_below_production_min")
        || blocker.contains("benchmark:batch_size_below_production_min")
        || blocker.contains("benchmark:payload_size_below_production_min")
        || blocker.contains("benchmark:report_required_workload_count_mismatch")
        || blocker.contains("benchmark:report_required_workload_missing")
        || blocker.contains("benchmark:report_duplicate_workload")
        || blocker.contains("benchmark:real_reference_raft_state_dir_create_failed")
        || blocker.contains("benchmark:real_reference_raft_harness_not_executable")
        || blocker.contains("benchmark:real_reference_raft_harness_metadata_failed")
        || blocker.contains("benchmark:real_reference_raft_harness_spawn_failed")
        || blocker.contains("benchmark:real_reference_raft_harness_failed")
        || blocker.contains("benchmark:real_reference_raft_harness_invalid_json")
        || blocker.contains("benchmark:real_reference_raft_harness_workload_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_engine_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_engine_source_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_implementation_mismatch")
        || blocker.contains("benchmark:reference_raft_implementation_mismatch")
        || blocker.contains("benchmark:rustraft_implementation_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_binary_path_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_git_revision_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_build_profile_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_node_count_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_iterations_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_batch_size_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_payload_size_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_operation_count_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_timed_iteration_count_mismatch")
        || blocker.contains(
            "benchmark:real_reference_raft_harness_operations_per_timed_iteration_mismatch",
        )
        || blocker.contains("benchmark:real_reference_raft_harness_total_duration_zero")
        || blocker
            .contains("benchmark:real_reference_raft_harness_total_duration_below_timed_iterations")
        || blocker.contains("benchmark:real_reference_raft_harness_throughput_duration_mismatch")
        || blocker.contains("benchmark:real_reference_raft_harness_p50_exceeds_total_duration")
        || blocker.contains("benchmark:real_reference_raft_harness_p99_exceeds_total_duration")
        || blocker.contains("benchmark:real_reference_raft_harness_p50_latency_zero")
        || blocker.contains("benchmark:real_reference_raft_harness_p99_latency_zero")
        || blocker.contains("benchmark:real_reference_raft_harness_latency_order_invalid")
        || blocker.contains("benchmark:real_reference_raft_harness_throughput_invalid")
        || blocker.contains("benchmark:reference_raft_sample_latency_order_invalid")
        || blocker.contains("benchmark:rustraft_sample_latency_order_invalid")
        || blocker.contains("benchmark:comparison_passed_with_blockers")
        || blocker.contains("benchmark:comparison_failed_without_blockers")
        || blocker.contains("benchmark:reference_raft_full_harness_missing")
        || blocker.contains("benchmark:rustraft_runtime_harness_missing")
        || blocker.contains("benchmark:reference_raft_provenance_")
        || blocker.contains("benchmark:rustraft_provenance_")
        || blocker.contains("benchmark:build_profile_mismatch")
        || blocker.contains("benchmark:binary_path_collision")
        || blocker.contains("benchmark:reference_raft_build_profile_not_release")
        || blocker.contains("benchmark:rustraft_build_profile_not_release")
        || blocker.contains("benchmark:summary_reference_raft_provenance_")
        || blocker.contains("benchmark:summary_rustraft_provenance_")
        || blocker.contains("benchmark:summary_reference_raft_implementation_mismatch")
        || blocker.contains("benchmark:summary_rustraft_implementation_mismatch")
        || blocker.contains("benchmark:summary_schema_mismatch")
        || blocker.contains("benchmark:summary_required_workloads_mismatch")
        || blocker.contains("benchmark:summary_workload_order_mismatch")
        || blocker.contains("benchmark:summary_run_id_missing")
        || blocker.contains("benchmark:summary_workload_passed_with_blockers")
        || blocker.contains("benchmark:summary_workload_failed_without_blockers")
        || blocker.contains("benchmark:summary_reference_raft_run_id_missing")
        || blocker.contains("benchmark:summary_reference_raft_run_id_mismatch")
        || blocker.contains("benchmark:summary_rustraft_run_id_missing")
        || blocker.contains("benchmark:summary_rustraft_run_id_mismatch")
        || blocker.contains("benchmark:summary_sample_run_id_pair_mismatch")
        || blocker.contains("benchmark:summary_build_profile_mismatch")
        || blocker.contains("benchmark:summary_binary_path_collision")
        || blocker.contains("benchmark:summary_reference_raft_build_profile_not_release")
        || blocker.contains("benchmark:summary_rustraft_build_profile_not_release")
        || blocker.contains("_generated_at_")
        || blocker.contains("benchmark:reference_raft_sample_")
        || blocker.contains("benchmark:rustraft_sample_")
        || blocker.contains("benchmark:comparison_")
        || blocker.contains("benchmark:summary_sample_")
        || blocker.contains("benchmark:summary_reference_raft_node_count_mismatch")
        || blocker.contains("benchmark:summary_rustraft_node_count_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_iterations_mismatch")
        || blocker.contains("benchmark:summary_rustraft_iterations_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_batch_size_mismatch")
        || blocker.contains("benchmark:summary_rustraft_batch_size_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_payload_size_mismatch")
        || blocker.contains("benchmark:summary_rustraft_payload_size_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_operation_count_mismatch")
        || blocker.contains("benchmark:summary_rustraft_operation_count_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_timed_iteration_count_mismatch")
        || blocker.contains("benchmark:summary_rustraft_timed_iteration_count_mismatch")
        || blocker
            .contains("benchmark:summary_reference_raft_operations_per_timed_iteration_mismatch")
        || blocker.contains("benchmark:summary_rustraft_operations_per_timed_iteration_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_total_duration_zero")
        || blocker.contains("benchmark:summary_rustraft_total_duration_zero")
        || blocker
            .contains("benchmark:summary_reference_raft_total_duration_below_timed_iterations")
        || blocker.contains("benchmark:summary_rustraft_total_duration_below_timed_iterations")
        || blocker.contains("benchmark:summary_reference_raft_throughput_duration_mismatch")
        || blocker.contains("benchmark:summary_rustraft_throughput_duration_mismatch")
        || blocker.contains("benchmark:summary_reference_raft_p50_exceeds_total_duration")
        || blocker.contains("benchmark:summary_rustraft_p50_exceeds_total_duration")
        || blocker.contains("benchmark:summary_reference_raft_p99_exceeds_total_duration")
        || blocker.contains("benchmark:summary_rustraft_p99_exceeds_total_duration")
        || blocker.contains("benchmark:summary_reference_raft_p50_latency_zero")
        || blocker.contains("benchmark:summary_reference_raft_p99_latency_zero")
        || blocker.contains("benchmark:summary_rustraft_p50_latency_zero")
        || blocker.contains("benchmark:summary_rustraft_p99_latency_zero")
        || blocker.contains("benchmark:summary_reference_raft_latency_order_invalid")
        || blocker.contains("benchmark:summary_rustraft_latency_order_invalid")
        || blocker.contains("benchmark:summary_reference_raft_throughput_invalid")
        || blocker.contains("benchmark:summary_rustraft_throughput_invalid")
        || blocker.contains("benchmark:summary_environment_fingerprint_missing")
        || blocker.contains("benchmark:summary_environment_debug_assertions_enabled")
        || blocker.contains("benchmark:summary_environment_release_fingerprint_missing")
        || blocker.contains("benchmark:summary_expected_")
        || blocker.contains("benchmark:summary_worst_")
        || blocker.contains("benchmark:summary_workload_expected_")
        || blocker.contains("benchmark:summary_workload_p50_ratio_not_finite")
        || blocker.contains("benchmark:summary_workload_p99_ratio_not_finite")
        || blocker.contains("benchmark:summary_workload_throughput_ratio_not_finite")
        || blocker.contains("benchmark:iterations_mismatch_reference_raft_")
        || blocker.contains("benchmark:batch_size_mismatch_reference_raft_")
        || blocker.contains("benchmark:payload_size_mismatch_reference_raft_")
        || blocker.contains("benchmark:operation_count_mismatch_reference_raft_")
        || blocker.contains("benchmark:timed_iteration_count_mismatch_reference_raft_")
        || blocker.contains("benchmark:operations_per_timed_iteration_mismatch_reference_raft_")
        || blocker.contains("benchmark:reference_raft_native_cluster_start_failed")
        || blocker.contains("benchmark:reference_raft_native_kvbench_build_blocked")
        || blocker.contains("benchmark:reference_raft_native_kvbench_parse_failed")
        || blocker.contains("benchmark:reference_raft_native_kvbench_zero_operations")
    {
        correctness_blockers.push(blocker.to_string());
    }
    if blocker.contains("benchmark:p50_regression")
        || blocker.contains("benchmark:p99_regression")
        || blocker.contains("benchmark:throughput_regression")
        || blocker.contains("benchmark:summary_p50_ratio_not_finite")
        || blocker.contains("benchmark:summary_p99_ratio_not_finite")
        || blocker.contains("benchmark:summary_throughput_ratio_not_finite")
        || blocker.contains("benchmark:summary_p50_regression")
        || blocker.contains("benchmark:summary_p99_regression")
        || blocker.contains("benchmark:summary_throughput_regression")
        || blocker.contains("benchmark:summary_workload_p50_ratio_mismatch")
        || blocker.contains("benchmark:summary_workload_p99_ratio_mismatch")
        || blocker.contains("benchmark:summary_workload_throughput_ratio_mismatch")
        || blocker.contains("benchmark:summary_workload_passed_despite_regression")
        || blocker.contains("benchmark:comparison_missing_p50_regression_blocker")
        || blocker.contains("benchmark:comparison_missing_p99_regression_blocker")
        || blocker.contains("benchmark:comparison_missing_throughput_regression_blocker")
        || blocker.contains("benchmark:comparison_passed_despite_regression")
    {
        performance_blockers.push(blocker.to_string());
    }
}

fn compare_samples(
    reference_raft: RustRaftBenchmarkSample,
    rustraft: RustRaftBenchmarkSample,
    tolerance_percent: f64,
) -> RustRaftBenchmarkComparison {
    let tolerance_valid = tolerance_percent.is_finite()
        && (0.0..=RUSTRAFT_BENCHMARK_MAX_PRODUCTION_PASS_TOLERANCE_PERCENT)
            .contains(&tolerance_percent);
    let max_latency_ratio = 1.0 + tolerance_percent / 100.0;
    let min_throughput_ratio = 1.0 - tolerance_percent / 100.0;
    let p50_ratio = ratio(
        rustraft.p50_latency_micros as f64,
        reference_raft.p50_latency_micros as f64,
    );
    let p99_ratio = ratio(
        rustraft.p99_latency_micros as f64,
        reference_raft.p99_latency_micros as f64,
    );
    let throughput_ratio = ratio(
        rustraft.throughput_ops_per_sec,
        reference_raft.throughput_ops_per_sec,
    );
    let mut blockers = Vec::new();

    blockers.extend(reference_raft.blockers.iter().cloned());
    blockers.extend(rustraft.blockers.iter().cloned());
    if !p50_ratio.is_finite() {
        blockers.push("benchmark:comparison_p50_ratio_not_finite".to_string());
    }
    if !p99_ratio.is_finite() {
        blockers.push("benchmark:comparison_p99_ratio_not_finite".to_string());
    }
    if !throughput_ratio.is_finite() {
        blockers.push("benchmark:comparison_throughput_ratio_not_finite".to_string());
    }
    if !reference_raft.correctness_passed {
        blockers.push("reference_raft_correctness_failed".to_string());
    }
    if !rustraft.correctness_passed {
        blockers.push("rustraft_correctness_failed".to_string());
    }
    if reference_raft.node_count != rustraft.node_count {
        blockers.push(format!(
            "node_count_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.node_count, rustraft.node_count
        ));
    }
    if reference_raft.iterations_per_workload != rustraft.iterations_per_workload {
        blockers.push(format!(
            "iterations_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.iterations_per_workload, rustraft.iterations_per_workload
        ));
    }
    if reference_raft.batch_size != rustraft.batch_size {
        blockers.push(format!(
            "batch_size_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.batch_size, rustraft.batch_size
        ));
    }
    if reference_raft.payload_size_bytes != rustraft.payload_size_bytes {
        blockers.push(format!(
            "payload_size_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.payload_size_bytes, rustraft.payload_size_bytes
        ));
    }
    if reference_raft.operation_count != rustraft.operation_count {
        blockers.push(format!(
            "operation_count_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.operation_count, rustraft.operation_count
        ));
    }
    if reference_raft.timed_iteration_count != rustraft.timed_iteration_count {
        blockers.push(format!(
            "timed_iteration_count_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.timed_iteration_count, rustraft.timed_iteration_count
        ));
    }
    if reference_raft.operations_per_timed_iteration != rustraft.operations_per_timed_iteration {
        blockers.push(format!(
            "operations_per_timed_iteration_mismatch_reference_raft_{}_rustraft_{}",
            reference_raft.operations_per_timed_iteration, rustraft.operations_per_timed_iteration
        ));
    }
    if reference_raft.implementation != RustRaftBenchmarkImplementation::ReferenceRaftCpp {
        blockers.push(format!(
            "reference_raft_implementation_mismatch_{}_reference_raft_cpp",
            reference_raft.implementation.id()
        ));
    }
    if rustraft.implementation != RustRaftBenchmarkImplementation::RustRaftRust {
        blockers.push(format!(
            "rustraft_implementation_mismatch_{}_rustraft_rust",
            rustraft.implementation.id()
        ));
    }
    if !tolerance_valid {
        blockers.push(format!(
            "benchmark:invalid_pass_tolerance_percent:{tolerance_percent:.3}"
        ));
    }
    if p50_ratio > max_latency_ratio {
        blockers.push(format!(
            "p50_ratio_{p50_ratio:.3}_exceeds_{max_latency_ratio:.3}"
        ));
    }
    if p99_ratio > max_latency_ratio {
        blockers.push(format!(
            "p99_ratio_{p99_ratio:.3}_exceeds_{max_latency_ratio:.3}"
        ));
    }
    if throughput_ratio < min_throughput_ratio {
        blockers.push(format!(
            "throughput_ratio_{throughput_ratio:.3}_below_{min_throughput_ratio:.3}"
        ));
    }

    RustRaftBenchmarkComparison {
        workload: reference_raft.workload,
        reference_raft,
        rustraft,
        p50_ratio,
        p99_ratio,
        throughput_ratio,
        passed: blockers.is_empty(),
        blockers,
    }
}

fn ratio(numerator: f64, denominator: f64) -> f64 {
    if denominator == 0.0 {
        return f64::INFINITY;
    }
    numerator / denominator
}

fn run_same_machine_model_workload(
    engine: RustRaftBenchmarkEngine,
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> RustRaftBenchmarkSample {
    let operation_count = match workload {
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options
            .iterations_per_workload
            .saturating_mul(options.batch_size),
        _ => options.iterations_per_workload,
    };
    let latency = synthetic_latency_series(engine, workload, options.iterations_per_workload);
    let p50_latency_micros = percentile(&latency, 50.0);
    let p99_latency_micros = percentile(&latency, 99.0);
    let total_micros = latency.iter().sum::<u64>().max(1);
    let throughput_ops_per_sec = throughput_from_duration(operation_count, total_micros);

    RustRaftBenchmarkSample {
        workload,
        engine,
        engine_source: RustRaftBenchmarkEngineSource::Model,
        benchmark_run_id: String::new(),
        implementation: RustRaftBenchmarkImplementation::Model,
        binary_path: None,
        git_revision: None,
        build_profile: "model".to_string(),
        harness_kind: RustRaftBenchmarkHarnessKind::Model,
        node_count: options.node_count,
        iterations_per_workload: options.iterations_per_workload,
        batch_size: options.batch_size,
        payload_size_bytes: options.payload_size_bytes,
        timed_iteration_count: latency.len(),
        operations_per_timed_iteration: writes_per_iteration(workload, options),
        total_duration_micros: total_micros,
        operation_count,
        p50_latency_micros,
        p99_latency_micros,
        throughput_ops_per_sec,
        correctness_passed: same_machine_correctness_passes(workload, options),
        blockers: Vec::new(),
    }
}

fn run_rustraft_runtime_workload(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
    runner: &RustRaftRuntimeBenchmarkRunner,
) -> RustRaftBenchmarkSample {
    let operation_count = operation_count_for(workload, options);
    let mut latencies = Vec::new();
    let correctness_passed = match workload {
        RustRaftBenchmarkWorkload::SingleKeyWrites
        | RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => {
            let mut cluster = benchmark_cluster(options.node_count);
            let payload = benchmark_payload(options);
            cluster.start().is_ok()
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    for _ in 0..writes_per_iteration(workload, options) {
                        cluster.propose(payload.clone())?;
                    }
                    Ok(())
                })
                .is_ok()
        }
        RustRaftBenchmarkWorkload::WalFsync => {
            let dir = temp_benchmark_dir("wal");
            let mut wal = PersistentRaftWal::open(PersistentRaftWalOptions {
                dir: dir.clone(),
                max_records_per_segment: 128,
                max_segment_bytes: 64 * 1024 * 1024,
                min_keep_segments: 2,
                fsync_on_append: true,
            })
            .expect("open benchmark WAL");
            let ok = run_timed(
                options.iterations_per_workload,
                &mut latencies,
                |iteration| {
                    let index = iteration as u64 + 1;
                    wal.append(benchmark_wal_record(index, benchmark_payload(options)))?;
                    Ok(())
                },
            )
            .is_ok();
            let _ = std::fs::remove_dir_all(dir);
            ok
        }
        RustRaftBenchmarkWorkload::ReadIndexReads | RustRaftBenchmarkWorkload::LeaseReads => {
            let mut cluster = benchmark_cluster(options.node_count);
            let started = cluster.start().is_ok() && cluster.propose(b"seed".to_vec()).is_ok();
            started
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    let response = cluster.read_index(crate::RustRaftReadIndexRequest {
                        group_id: 10,
                        requester_id: 1,
                        min_commit_index: 1,
                        allow_lease_read: matches!(workload, RustRaftBenchmarkWorkload::LeaseReads),
                    })?;
                    if !response.safe {
                        return Err(RaftError::InvalidRequest(response.reason));
                    }
                    Ok(())
                })
                .is_ok()
        }
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup
        | RustRaftBenchmarkWorkload::SnapshotStreaming => {
            let mut cluster = benchmark_cluster(options.node_count);
            let started = cluster.start().is_ok();
            let payload = benchmark_payload(options);
            started
                && run_timed(
                    options.iterations_per_workload,
                    &mut latencies,
                    |iteration| {
                        let index = iteration as u64 + 1;
                        cluster.install_snapshot_with_tail_to(
                            2,
                            crate::RaftSnapshot {
                                group_id: 10,
                                meta: RustRaftSnapshotMeta {
                                    snapshot_id: format!("bench-snap-{index}"),
                                    last_log_id: RustRaftLogId { term: 1, index },
                                    membership: benchmark_voters(options.node_count),
                                    members: Vec::new(),
                                },
                                payload: payload.clone(),
                            },
                            RustRaftApplySnapshotFence {
                                applied_index: index,
                                commit_index: index,
                                installed_snapshot_index: index,
                                first_retained_log_index: index + 1,
                            },
                            Vec::new(),
                        )?;
                        Ok(())
                    },
                )
                .is_ok()
        }
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad => {
            let mut cluster = benchmark_cluster(options.node_count);
            let started = cluster.start().is_ok();
            let payload = benchmark_payload(options);
            started
                && run_timed(options.iterations_per_workload, &mut latencies, |_| {
                    cluster.propose(payload.clone())?;
                    let target = if cluster.leader_id() == Some(1) { 2 } else { 1 };
                    cluster.transfer_leader(target)?;
                    Ok(())
                })
                .is_ok()
        }
    };
    let total_micros = latencies.iter().sum::<u64>().max(1);
    let throughput_ops_per_sec = throughput_from_duration(operation_count, total_micros);
    RustRaftBenchmarkSample {
        workload,
        engine: RustRaftBenchmarkEngine::RustRaft,
        engine_source: RustRaftBenchmarkEngineSource::RustRaftRuntime,
        benchmark_run_id: String::new(),
        implementation: RustRaftBenchmarkImplementation::RustRaftRust,
        binary_path: runner.binary_path(),
        git_revision: runner.git_revision(),
        build_profile: runner.build_profile(),
        harness_kind: RustRaftBenchmarkHarnessKind::RustRaftRuntime,
        node_count: options.node_count,
        iterations_per_workload: options.iterations_per_workload,
        batch_size: options.batch_size,
        payload_size_bytes: options.payload_size_bytes,
        timed_iteration_count: latencies.len(),
        operations_per_timed_iteration: writes_per_iteration(workload, options),
        total_duration_micros: total_micros,
        operation_count,
        p50_latency_micros: percentile(&latencies, 50.0),
        p99_latency_micros: percentile(&latencies, 99.0),
        throughput_ops_per_sec,
        correctness_passed,
        blockers: Vec::new(),
    }
}

fn run_timed(
    iterations: usize,
    latencies: &mut Vec<u64>,
    mut operation: impl FnMut(usize) -> Result<(), RaftError>,
) -> Result<(), RaftError> {
    for iteration in 0..iterations.max(1) {
        let start = Instant::now();
        operation(iteration)?;
        latencies.push(start.elapsed().as_micros().max(1) as u64);
    }
    Ok(())
}

fn operation_count_for(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> usize {
    match workload {
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options
            .iterations_per_workload
            .saturating_mul(options.batch_size),
        _ => options.iterations_per_workload,
    }
}

fn writes_per_iteration(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> usize {
    match workload {
        RustRaftBenchmarkWorkload::BatchedWrites
        | RustRaftBenchmarkWorkload::ReplicationBatching => options.batch_size.max(1),
        _ => 1,
    }
}

fn benchmark_payload(options: &RustRaftBenchmarkOptions) -> Vec<u8> {
    vec![42; options.payload_size_bytes.max(1)]
}

fn benchmark_cluster(node_count: usize) -> RaftCluster {
    RaftCluster::new(
        10,
        RaftConfig::default(),
        benchmark_voters(node_count.max(3))
            .into_iter()
            .map(benchmark_peer)
            .collect(),
    )
    .expect("benchmark cluster")
}

fn benchmark_voters(node_count: usize) -> Vec<u64> {
    (1..=node_count.max(3) as u64).collect()
}

fn benchmark_peer(node_id: u64) -> RustRaftPeer {
    RustRaftPeer {
        node_id,
        raft_addr: format!("127.0.0.1:{}", 40_000 + node_id),
        snapshot_addr: format!("127.0.0.1:{}", 41_000 + node_id),
        role: RustRaftReplicaRole::Voter,
        auto_promote: false,
    }
}

fn benchmark_wal_record(index: u64, payload: Vec<u8>) -> RustRaftWalRecord {
    RustRaftWalRecord {
        group_id: 10,
        node_id: 1,
        hard_state: RustRaftHardState {
            current_term: 1,
            voted_for: Some(1),
            committed: Some(RustRaftLogId { term: 1, index }),
        },
        membership: RustRaftMembership {
            group_id: 10,
            voters: benchmark_voters(3),
            learners: Vec::new(),
            witnesses: Vec::new(),
            epoch: 1,
        },
        entries: vec![RustRaftLogEntry {
            log_id: RustRaftLogId { term: 1, index },
            payload,
            is_command: true,
        }],
        installed_snapshot: None,
        apply_snapshot_fence: RustRaftApplySnapshotFence {
            applied_index: index,
            commit_index: index,
            installed_snapshot_index: 0,
            first_retained_log_index: 1,
        },
        checksum: String::new(),
    }
}

fn temp_benchmark_dir(name: &str) -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "rustraft-benchmark-{name}-{}-{nonce}",
        std::process::id()
    ))
}

fn git_revision_for(root: &Path) -> Result<String, String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .arg("rev-parse")
        .arg("HEAD")
        .output()
        .map_err(|err| format!("git_revision_unavailable:{err}"))?;
    if !output.status.success() {
        return Err("git_revision_unavailable".to_string());
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn synthetic_latency_series(
    engine: RustRaftBenchmarkEngine,
    workload: RustRaftBenchmarkWorkload,
    iterations: usize,
) -> Vec<u64> {
    let base = match workload {
        RustRaftBenchmarkWorkload::SingleKeyWrites => 900,
        RustRaftBenchmarkWorkload::BatchedWrites => 1_600,
        RustRaftBenchmarkWorkload::ReplicationBatching => 1_250,
        RustRaftBenchmarkWorkload::WalFsync => 2_200,
        RustRaftBenchmarkWorkload::ReadIndexReads => 320,
        RustRaftBenchmarkWorkload::LeaseReads => 120,
        RustRaftBenchmarkWorkload::SnapshotInstallCatchup => 8_000,
        RustRaftBenchmarkWorkload::SnapshotStreaming => 6_500,
        RustRaftBenchmarkWorkload::LeaderTransferUnderLoad => 4_500,
    };
    let engine_multiplier = match engine {
        RustRaftBenchmarkEngine::ReferenceRaft => 100,
        RustRaftBenchmarkEngine::RustRaft => 108,
    };
    (0..iterations.max(1))
        .map(|index| {
            let jitter = ((index as u64 * 37) % 17) * 3;
            (base + jitter) * engine_multiplier / 100
        })
        .collect()
}

fn percentile(values: &[u64], percentile: f64) -> u64 {
    let mut values = values.to_vec();
    values.sort_unstable();
    let last = values.len().saturating_sub(1);
    let rank = ((percentile / 100.0) * last as f64).ceil() as usize;
    values[rank.min(last)]
}

fn same_machine_correctness_passes(
    workload: RustRaftBenchmarkWorkload,
    options: &RustRaftBenchmarkOptions,
) -> bool {
    let production_scale = options.node_count >= RUSTRAFT_BENCHMARK_MIN_PRODUCTION_NODE_COUNT;
    let iterations_present = options.iterations_per_workload > 0;
    let payload_present =
        options.payload_size_bytes >= RUSTRAFT_BENCHMARK_MIN_PRODUCTION_PAYLOAD_SIZE_BYTES;
    let batch_valid = !matches!(
        workload,
        RustRaftBenchmarkWorkload::BatchedWrites | RustRaftBenchmarkWorkload::ReplicationBatching
    ) || options.batch_size > 1;
    production_scale && iterations_present && payload_present && batch_valid
}
