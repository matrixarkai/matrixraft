# MatrixRaft

[![CI](https://github.com/bjmeetsfo/MatrixRaft/actions/workflows/ci.yml/badge.svg)](https://github.com/bjmeetsfo/MatrixRaft/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.82-blue.svg)](Cargo.toml)

MatrixRaft is the TemporalStore-owned Rust Raft readiness and parity contract
library. It is intentionally small: the crate owns the stable public contract
for Raft semantic requirements, readiness evidence, and parity reports, while
TemporalStore owns the storage runtime, data-node integration, and metaserver
integration.

The Cargo crate is named `matrixraft`, and the naming follows from that:

- **Types are unprefixed** — `Storage`, `Message`, `Config`, not
  `MatrixRaftStorage`. The module path already says which crate they belong to,
  which is the shape `raft-rs` uses for `raft::Storage`. Where a concept has an
  established upstream name, that name is used: `StateRole`, `ProgressState`,
  `SnapshotMetadata`.
- **Evidence records are canonical when they gate release decisions** —
  `SnapshotLifecycleEvidence` is named as a Rust evidence type and mapped to the
  TiKV/raft-rs and ByteRaft/BaselineRaft vocabulary because it controls
  production snapshot lifecycle readiness.
- **Free functions carry the crate name** — `matrixraft_parity_report`,
  `matrixraft_production_readiness_report` — because they are imported into a
  consumer's namespace, where a bare name would not say where it came from.
- **The compatibility facade keeps its `MatrixRaft*` prefix**, because those
  types mirror the reference implementation's API rather than this crate's, and
  several are genuinely distinct from the like-named type here: `NodeId` is a
  `u64` alias while `MatrixRaftNodeId` is a struct of a peer id and two
  addresses.
- **Emitted strings are not identifiers.** Prometheus metric names, alert rule
  names and evidence keys keep their `rustraft_*` / `RustRaft*` spelling. They
  are a published interface that dashboards and alert rules are built from, so
  renaming them is an operational change rather than a tidy-up.
- **Reference vocabulary is explicit.** `matrixraft_api_name_mappings()` is the
  translation table for TiKV/raft-rs, MatrixRaft facade, and ByteRaft/BaselineRaft
  concepts so reviewers do not have to infer whether a name is canonical Rust API,
  compatibility facade, metric surface, or reference-system vocabulary.

License: Apache-2.0.

## What This Crate Provides

- `SemanticRequirement`
- `ParityContract`
- `ParityReport`
- `ProductionReadinessInput`
- `ProductionReadinessReport`
- `ProcessRolloutReadinessReport`
- `ProductionStatus`
- `Storage`
- `Transport`
- `InMemoryRaftTransport`
- `TransportValidationReport`
- `StatusSnapshot`
- `MetricNames`
- `FaultScenario`
- `matrixraft_fault_harness_readiness_report`
- `matrixraft_read_safety_decision`
- `matrixraft_applied_index_fence_report`
- `matrixraft_lease_read_eligibility_report`
- `matrixraft_bounded_stale_read_report`
- `matrixraft_learner_promotion_decision`
- `matrixraft_append_safety_decision`
- `ReadinessEvidence`
- `ReadinessSnapshot`
- `matrixraft_parity_contract`
- `matrixraft_parity_report`
- `matrixraft_production_readiness_report`
- `matrixraft_production_readiness_report_with_runtime_pressure_policy`
- `matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness`
- `matrixraft_production_readiness_report_prometheus`
- `matrixraft_production_readiness_metric_names`
- `matrixraft_production_readiness_grafana_panels`
- `ProductionReadinessMetricNames`
- `matrixraft_data_node_process_rollout_readiness_report`
- `matrixraft_meta_process_rollout_readiness_report`
- `matrixraft_baseline_raft_runtime_capability_report`
- `matrixraft_baseline_raft_runtime_capability_prometheus`
- `matrixraft_baseline_raft_benchmark_grafana_panels`
- `matrixraft_baseline_raft_benchmark_metric_names`
- `matrixraft_baseline_raft_benchmark_summary_prometheus`
- `matrixraft_benchmark_runbook_steps`
- `matrixraft_release_benchmark_runtime_timer_status`
- `matrixraft_read_release_pressure_snapshot`
- `matrixraft_write_release_pressure_snapshot_atomic`
- `matrixraft_benchmark_runtime_pressure_readiness_artifact`
- `matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
- `matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
- `matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact`
- `matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
- `matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
- `matrixraft_validate_benchmark_runtime_pressure_readiness_artifact`
- `matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
- `matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
- `matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact`
- `matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
- `matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
- `matrixraft_production_readiness_input_with_benchmark_artifacts`
- `matrixraft_production_readiness_input_with_asserted_benchmark_artifacts`
- `matrixraft_production_readiness_report_with_benchmark_artifacts`
- `matrixraft_production_readiness_report_with_asserted_benchmark_artifacts`
- `matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts`
- `matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts`
- `matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts`
- `matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
- `matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts`
- `matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
- `matrixraft_scale_optimization_inputs_from_benchmark_report`
- `matrixraft_validate_benchmark_scale_optimization_inputs`
- `matrixraft_scale_rate_metrics_from_benchmark_report`
- `matrixraft_scale_optimization_targets_from_baseline_raft_report`
- `matrixraft_public_api_contract`
- `matrixraft_public_api_contract_validation_prometheus`
- `matrixraft_public_api_contract_validation_grafana_panels`
- `matrixraft_api_name_mappings`
- `matrixraft_core_interface_names`
- `matrixraft_evidence_interface_names`
- `matrixraft_open_source_surface`
- `matrixraft_temporalstore_adapter_shape`
- `matrixraft_temporalstore_extraction_plan`
- `matrixraft_metric_names`
- `matrixraft_grafana_dashboard`
- `matrixraft_grafana_dashboard_json`
- `matrixraft_latency_metrics_prometheus`
- `matrixraft_memory_metric_names`
- `matrixraft_memory_metrics_prometheus`
- `matrixraft_memory_optimization_hints`
- `matrixraft_runtime_pressure_admission`
- `matrixraft_runtime_pressure_admission_with_scale_targets`
- `matrixraft_runtime_pressure_admission_with_pipeline_pressure`
- `matrixraft_runtime_pressure_admission_with_read_backlog_pressure`
- `matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure`
- `matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure`
- `matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure`
- `matrixraft_runtime_pressure_admission_with_scale_pipeline_read_backlog_and_node_runtime_timer_pressure`
- `matrixraft_runtime_pressure_freshness_report`
- `matrixraft_runtime_pressure_freshness_prometheus`
- `matrixraft_runtime_pressure_admission_prometheus`
- `matrixraft_runtime_pressure_metric_names`
- `matrixraft_runtime_pressure_grafana_panels`
- `matrixraft_debug_snapshot_with_runtime_pressure_evidence`
- `matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence`
- `matrixraft_debug_snapshot_with_runtime_pressure_read_backlog_and_node_runtime_timer_evidence`
- `RuntimePressureAdmission`
- `RuntimePressureAdmissionPolicy`
- `matrixraft_validate_runtime_pressure_admission_evidence_with_policy`
- `RuntimePressureMetricNames`
- `ReadBacklogMetrics`
- `ReadBacklogThresholds`
- `NodeRuntimeTimerPressureDetail`
- `NodeRuntimeTimerThresholds`
- `matrixraft_membership_readiness_metric_names`
- `matrixraft_membership_readiness_prometheus`
- `matrixraft_membership_readiness_grafana_panels`
- `MembershipReadinessMetricNames`
- `matrixraft_scale_metric_names`
- `matrixraft_scale_metrics_prometheus`
- `matrixraft_scale_target_metric_names`
- `matrixraft_scale_target_metrics_prometheus`
- `matrixraft_scale_target_grafana_panels`
- `matrixraft_scale_optimization_hints`
- `matrixraft_alert_rules`
- `matrixraft_alert_rules_json`
- `matrixraft_diagnostic_log_prometheus`
- `matrixraft_observability_provisioning`
- `matrixraft_observability_provisioning_json`
- `matrixraft_observability_required_metric_names`
- `matrixraft_validate_required_metric_scrape_texts`
- `matrixraft_observability_provisioning_runbook_steps`
- `matrixraft_observability_provisioning_validation_prometheus`
- `matrixraft_validate_observability_provisioning`
- `matrixraft_validate_observability_provisioning_json`
- `matrixraft_operator_triage_summary`
- `matrixraft_operator_triage_prometheus`
- `matrixraft_operator_runbook_steps`
- `matrixraft_operator_runbook_steps_with_diagnostics`
- `matrixraft_operator_runbook_prometheus`
- `matrixraft_debug_bundle_contract`
- `matrixraft_validate_debug_snapshot`
- `matrixraft_validate_debug_snapshot_json`
- `matrixraft_debug_bundle_validation_prometheus`
- `matrixraft_debug_snapshot_with_benchmark_scale_inputs`
- `matrixraft_debug_snapshot_with_benchmark_summary`
- `matrixraft_debug_snapshot_with_benchmark_artifacts`
- `matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts`
- `matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
- `matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
- `matrixraft_debug_snapshot_with_scale_metrics`
- `matrixraft_debug_snapshot_with_runtime_metrics`
- `matrixraft_debug_snapshot_with_observability_metrics`
- `matrixraft_debug_snapshot_with_performance_targets`
- `matrixraft_admin_diagnostic_log_entries`
- `matrixraft_admin_diagnostic_json_lines`
- `matrixraft_local_status_diagnostic_log_entries`
- `matrixraft_local_status_diagnostic_json_lines`
- `matrixraft_node_runtime_status_diagnostic_log_entries`
- `matrixraft_node_runtime_status_diagnostic_json_lines`
- `matrixraft_optimization_diagnostic_log_entries`
- `matrixraft_optimization_diagnostic_json_lines`
- `matrixraft_runtime_pressure_diagnostic_log_entries`
- `matrixraft_runtime_pressure_diagnostic_json_lines`
- `matrixraft_runtime_pressure_freshness_diagnostic_log_entries`
- `matrixraft_runtime_pressure_freshness_diagnostic_json_lines`
- `matrixraft_node_runtime_status_prometheus`
- `matrixraft_node_runtime_grafana_panels`
- `matrixraft_membership_readiness_diagnostic_log_entries`
- `matrixraft_membership_readiness_diagnostic_json_lines`
- `matrixraft_production_readiness_diagnostic_log_entries`
- `matrixraft_production_readiness_diagnostic_json_lines`
- `matrixraft_optimization_report`
- `matrixraft_optimization_report_prometheus`
- `matrixraft_debug_snapshot_json`
- `matrixraft_debug_snapshot_metadata_prometheus`
- `matrixraft_peer_pipeline_metrics_prometheus`

Run `cargo run --example debug_artifacts` to print a complete sample support
artifact with the debug snapshot, admin and local-status JSON log lines,
runtime-pressure freshness JSON log lines, diagnostic, peer-pipeline,
runtime-pressure freshness, and optimization Prometheus text, runbook
Prometheus text, Grafana dashboard JSON, alert-rule JSON, validation report, and
validation Prometheus series in one envelope.

The crate is OpenRaft-free and independent of OpenRaft types. TemporalStore
converts its internal readiness evidence into `ReadinessSnapshot` or
implements `ReadinessEvidence`, then asks this crate to build a
conservative parity report.

## Production Readiness Status

`matrixraft_parity_report` returns both a compatibility boolean and an explicit
production status:

- `blocked`: at least one required safety, durability, transport, snapshot,
  membership, or observability requirement is missing.
- `feature_correct`: the contract shape is usable, but the runtime evidence is
  not enough to claim production readiness.
- `production_ready`: every required semantic is present, OpenRaft is absent
  from the public contract, and the TemporalRaft runtime is available.

Reports include `production_blockers` such as
`durability:storage_apply_fence`, making missing production evidence easy to
surface in TemporalStore readiness gates and CI.
Production readiness now also runs `matrixraft_validate_public_api_contract` on
the embedded public API contract, so required TiKV/ByteRaft reference mappings
show up as `public_api:*` missing evidence and blockers before RustRaft can be
called production ready.
Production readiness also requires an accepted runtime-pressure admission sample
with no memory, p99 latency, scale-target, peer-pipeline, read-backlog,
node-runtime timer, or pending-action pressure, so QPS/latency parity cannot
pass while the serving runtime is already asking operators to tune or shed work.
Use `matrixraft_production_readiness_input_with_runtime_pressure_evidence` to
attach release-scale memory, latency, QPS/throughput target, and per-peer
pipeline evidence directly to `ProductionReadinessInput`; it computes the same
fail-closed admission decision used by the runtime-pressure dashboards.
Use `matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts`
when the QPS/throughput rates and targets should come directly from a validated
BaselineRaft benchmark report and summary.
Use
`matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts`
when release automation must reject matched-but-failing benchmark artifacts
before benchmark-derived scale targets are trusted.
Use
`matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
when those benchmark-derived targets must be gated with live pending ReadIndex
and bounded-stale read backlog before building production readiness evidence.
Use the corresponding
`matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts`
helper when read-heavy release automation must reject failing benchmark parity
before pending-read backlog evidence is considered.
Use
`matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
when the same release-scale path must also fail closed on node-runtime timer
queue saturation.
Use the corresponding
`matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
helper when timer-sensitive release automation must prove benchmark parity
before timer-pressure evidence is trusted.
The fail-closed report names that requirement as
`runtime_pressure:no_read_backlog_pressure`, so release automation and on-call
triage can distinguish read-backlog pressure from generic admission rejection.
Use `matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts`
when release automation should produce the fail-closed report directly from
those benchmark artifacts and live runtime-pressure inputs.
Use
`matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts`
when the report itself must not be built until the artifacts prove clean QPS,
latency, throughput, correctness, CPU, and memory parity.
Use
`matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
for the same one-call production gate when read backlog pressure is part of the
release signal.
Use
`matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_and_read_backlog_artifacts`
when that read-backlog report must reject failing benchmark artifacts before
building the report.
Use
`matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
when the one-call production gate must combine benchmark-derived QPS targets,
read backlog, peer pipeline, and timer saturation evidence.
Use
`matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
when that complete-pressure report must prove clean benchmark parity before
read-backlog or timer signals are evaluated.
Use `matrixraft_benchmark_runtime_pressure_readiness_artifact` when the release
job also needs the matching Prometheus readiness metrics, runtime-pressure
Prometheus gauges, and structured diagnostic JSON lines from the same inputs.
Use `matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
when that serialized artifact must include read backlog pressure in the same
fail-closed runtime decision.
Use
`matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
when the serialized artifact must also preserve node-runtime timer saturation
and its `rustraft_runtime_pressure_*` gauges in the same promotion evidence.
Validate the timer-aware serialized artifact with
`matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
before a release job trusts it; the validator rejects schema, freshness, report,
structured-label, readiness-Prometheus, runtime-pressure-Prometheus,
read-backlog, timer-pressure, and diagnostic-log drift.
Runtime-pressure scrape text is also checked for format, metric-count,
well-formed samples and label sets, and exact accepted/rejected admission metric
sample names before promotion can trust it.
The timer-aware validator additionally requires the
`rustraft_runtime_pressure_node_runtime_timer` metric, so release automation can
distinguish missing timer evidence from generic scrape drift.
Use
`matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
for artifacts produced by
`matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`, so
promotion checks recompute the same read-backlog-aware pressure evidence instead
of the zero-backlog compatibility path.
The BaselineRaft runtime capability report exports the same requirement as
`runtime_pressure_admission_gate`, making this guard visible beside WAL,
snapshot, process, and benchmark evidence.

`matrixraft_production_readiness_report` is the fail-closed deployment gate. It
wraps the semantic parity report with runtime evidence for peer pipeline,
snapshot lifecycle, WAL lifecycle, data-node rollout, metaserver rollout,
admin/status observability, fault harness results, and real BaselineRaft benchmark
parity.
`matrixraft_production_readiness_report_prometheus` renders that exact gate as
canonical `rustraft_production_readiness_*` gauges so release automation can
alert on missing evidence, blockers, and recommended next actions. The same
metric names are exported through `matrixraft_production_readiness_metric_names`
and surfaced by `matrixraft_production_readiness_grafana_panels`, keeping the
dashboard, alerts, and scrape payload tied to one contract.
`matrixraft_production_readiness_diagnostic_json_lines` emits the same
ready/missing/blocker/action state as structured log entries for on-call
pipelines that route JSON logs instead of Prometheus alerts. Call
`matrixraft_operator_runbook_steps_with_diagnostics` with those entries to add a
dedicated `resolve_production_readiness_blockers` remediation step.
The data-node and metaserver rollout report helpers expose the same fail-closed
process-path checks independently, so TemporalStore and downstream adopters can
validate spawned-process evidence before composing the full production report.
`matrixraft_baseline_raft_runtime_capability_report` groups the same evidence into
BaselineRaft-derived runtime capability families: process-path rollout proof,
per-peer replication pipeline state, reorder queues, snapshot sender/downloader
lifecycle, WAL segment lifecycle, read-index/lease safety, membership role
semantics, FSM apply atomicity, and admin/metrics observability.
Pipeline evidence must prove packet-loss probe behavior and recovery, plus
reorder handling and convergence after reordered appends, before production
readiness can pass.
It must also prove at least one peer recovers after seeing both packet loss and
reordered append pressure, so split-peer evidence cannot satisfy the gate.
Snapshot lifecycle evidence must prove sustained sender/downloader load reaches
sender ack completion and downloader install completion on a coherent transfer
path, not only partial progress counters or split-peer fragments. The same
artifact also reports `snapshot_peer_count` and
`sustained_transfer_completed_peer_count`, so release automation can distinguish
a single completed transfer from quorum-sized or all-peer snapshot evidence in
larger groups.
Membership evidence must prove joint-consensus commits with both old and new
quorum acknowledgements for voter-changing scale-up and scale-down transitions.
WAL lifecycle evidence must prove segment compaction after slow-fsync pressure,
not only slow-fsync and released-segment counters observed independently.
`matrixraft_baseline_raft_runtime_capability_prometheus` renders that report as generic
`rustraft_baseline_raft_*` Prometheus text metrics. Product runtimes such as
TemporalStore can attach their own service labels without duplicating the
capability-matrix logic.
`matrixraft_grafana_dashboard_json` renders a deterministic Grafana dashboard
contract for the canonical `rustraft_*` readiness, QPS, throughput, latency,
queue, WAL, snapshot, blocker, and fatal-event metrics.
`matrixraft_latency_metrics_prometheus` renders canonical histogram bucket,
sum, and count series for append, vote, pre-vote, read-index, and snapshot
install latency panels.
`matrixraft_memory_metrics_prometheus` renders resident, heap, log-cache,
snapshot-buffer, and replication-buffer memory gauges for leak, cache, and
backpressure triage under release-scale workloads.
`matrixraft_memory_optimization_hints` classifies those gauges against
operator thresholds so memory pressure becomes warning-level optimization
guidance in debug snapshots before it turns into rejected work.
`matrixraft_runtime_pressure_admission` combines those memory gauges with
latency histogram p99 estimates and returns a serializable admission decision.
`matrixraft_runtime_pressure_admission_with_scale_targets` additionally folds
release-scale QPS and throughput target misses into the same pressure decision.
`matrixraft_runtime_pressure_admission_with_pipeline_pressure` and
`matrixraft_runtime_pressure_admission_with_scale_and_pipeline_pressure` fold
per-peer append/apply/reorder queue pressure and replication backpressure
rejections into the same decision, so production services can fail closed before
pipeline buildup quietly invalidates QPS and p99 latency evidence.
`matrixraft_runtime_pressure_admission_with_read_backlog_pressure` folds pending
ReadIndex and bounded-stale read backlog into the same decision, so read-heavy
clusters can shed, route, or deadline reads before pending queues invalidate
read-index QPS and tail-latency claims.
`matrixraft_runtime_pressure_admission_with_node_runtime_timer_pressure` folds
pending tick queue utilization into the same decision, so scheduler saturation
blocks production-readiness and QPS/latency parity claims before ticks are
rejected or delayed.
`matrixraft_runtime_pressure_admission_with_scale_pipeline_and_read_backlog_pressure`
is the full production admission helper for release-scale readers: it combines
scale targets, per-peer pipeline pressure, and pending read backlog in one
fail-closed decision.
`matrixraft_runtime_pressure_freshness_report` classifies runtime-pressure
evidence as fresh, low-fresh, stale, or invalid before release automation trusts
memory, latency, and QPS samples for production-readiness decisions.
`matrixraft_production_readiness_report_with_runtime_pressure_policy_and_freshness`
turns that freshness state into the production-readiness gate, blocking stale,
invalid, or future-dated QPS/latency/memory pressure evidence before a release
claim can pass.
`matrixraft_runtime_pressure_freshness_prometheus` exports that evidence age,
stale boundary, remaining freshness, status, and issue count so Grafana can
guard release dashboards against stale runtime-pressure samples.
Benchmark runtime-pressure readiness artifacts embed the same freshness
Prometheus text beside runtime-pressure admission metrics, so Grafana and
release automation can inspect the exact freshness gate carried by the embedded
production-readiness report.
The default policy is observe-only for dashboards and release dry runs;
`RuntimePressureAdmissionPolicy::fail_closed()` lets production services reject
new work when memory, p99 latency, scale target, peer pipeline, read-backlog, or
node-runtime timer pressure crosses configured thresholds.
Malformed latency histograms, such as non-monotonic bucket counts, invalid
bucket bounds, or missing/mismatched `+Inf` buckets, are treated as latency
pressure for the affected component so fail-closed admission does not silently
trust incomplete release evidence.
`matrixraft_runtime_pressure_admission_prometheus` and
`matrixraft_runtime_pressure_grafana_panels` expose those decisions as
canonical `rustraft_runtime_pressure_*` accepted/rejected, memory-pressure,
latency-pressure, scale-pressure, pipeline-pressure, read-backlog-pressure,
node-runtime timer-pressure, and action metrics for rollout dashboards and
alerts.
`matrixraft_membership_readiness_prometheus` and
`matrixraft_membership_readiness_grafana_panels` expose metaserver and data-node
failover, scale-up, and scale-down readiness as canonical
`rustraft_membership_*` metrics so joint-consensus, learner catch-up, witness,
and scheduler-generation gaps show up before a rollout claims production parity.
`matrixraft_scale_metrics_prometheus` renders proposal, AppendEntries,
ReadIndex, apply, replication-byte, and apply-byte counters for release-scale
QPS and throughput dashboards.
`matrixraft_scale_target_metrics_prometheus` renders caller-supplied
release-scale minimum targets plus observed target-attainment percentages, and
`matrixraft_scale_target_grafana_panels` adds those percentages to the dashboard
so operators can see whether a run is below, at, or above parity target.
`matrixraft_scale_optimization_hints` classifies caller-derived proposal,
AppendEntries, ReadIndex, apply, replication, and apply-throughput rates against
explicit minimum targets. Default targets are disabled so runtimes do not invent
machine-independent QPS claims; release benchmarks and production rollouts can
opt in with workload-specific targets.
`matrixraft_peer_pipeline_metrics_prometheus` renders per-peer append queue,
reorder queue, reorder-convergence, and snapshot-index metrics from a local
runtime status report so the corresponding Grafana panels have scrapeable data.
`matrixraft_admin_diagnostic_json_lines` renders the admin report as structured
JSON log lines for embedders that want RustRaft-owned debugging context without
adopting a specific logging crate.
`matrixraft_local_status_diagnostic_json_lines` renders local node, replication,
apply, peer-pipeline, and blocker diagnostics as structured JSON lines for
per-node log pipelines.
`matrixraft_node_runtime_status_diagnostic_json_lines` renders live node-runtime
timer and tick-backpressure counters as structured JSON lines for scheduler
starvation and timer-loop triage.
`matrixraft_node_runtime_status_prometheus` renders the same live timer counters
as scrapeable metrics for dashboards and alerts.
`matrixraft_node_runtime_grafana_panels` adds dashboard panels for timer-loop
pending/admitted/completed/rejected ticks and backpressure.
`matrixraft_alert_rules` includes `RustRaftNodeRuntimeTimerBackpressure`, and
`matrixraft_operator_runbook_steps` emits
`resolve_node_runtime_timer_backpressure` so sustained pending or rejected timer
ticks become an actionable operator signal before release-scale latency or QPS
claims are trusted.
`RustRaftMemoryPressure` and `RustRaftLatencyPressure` emit dedicated
`resolve_memory_pressure` and `resolve_latency_pressure` runbook steps so
resident/heap/cache/snapshot/replication memory and append/vote/read/snapshot
p99 latency must be cleared before release-scale QPS or latency parity is
trusted.
`RustRaftRuntimePressureBottleneckActive` warns when ranked bottleneck scores
are nonzero, and `inspect_runtime_pressure_bottleneck_warning` points operators
at Runtime Pressure Bottlenecks and Runtime Pressure Action Sources before
observe-only pressure is allowed into QPS, latency, or memory parity claims.
`RustRaftRuntimePressureFreshnessLow`,
`RustRaftRuntimePressureFreshnessLost`, and
`RustRaftRuntimePressureFreshnessInvalid` guard the age and shape of
release-scale runtime-pressure samples, with
`refresh_runtime_pressure_evidence` requiring fresh QPS, latency, and memory
evidence before parity dashboards are accepted.
The same alert/runbook contract covers `RustRaftSnapshotRetryBackpressure` and
`RustRaftWalSlowFsyncBackpressure`, tying snapshot transfer retry pressure and
WAL slow-fsync pressure back to lifecycle panels before durability, catch-up, or
write-latency parity is claimed.
`RustRaftPeerPipelineBackpressure` similarly guards per-peer append and reorder
queue depth, with `resolve_peer_pipeline_backpressure` pointing operators at
append queue, reorder queue, convergence, and snapshot catch-up signals before
QPS or p99 latency parity is accepted.
`RustRaftRuntimeReadBacklogPressure` guards pending ReadIndex and bounded-stale
read queues, with `resolve_read_backlog_pressure` pointing operators at runtime
read-backlog panels, read-index p99 latency, and deadline-bound replica reads
before release-scale read QPS or random-replica reads are trusted.
`RustRaftMembershipTransitionMissing` guards missing membership evidence across
metaserver and data-node failover/scale transitions, with
`resolve_membership_transition_evidence` covering joint consensus, learner
catch-up, witness quorum, and scheduler-generation gaps.
`matrixraft_optimization_diagnostic_json_lines` renders optimization hints as
the same diagnostic JSON-line schema, mapping critical, warning, and info hints
to error, warn, and info log severities for QPS, latency, memory, WAL, and
replication triage pipelines.
`matrixraft_runtime_pressure_diagnostic_json_lines` renders pressure admission
decisions as structured JSON lines with `rustraft.runtime_pressure.admission`
targets so rejected work, observe-only pressure, and suggested actions land in
the same log pipeline as admin and optimization diagnostics. It also emits
per-component `rustraft.runtime_pressure.memory`,
`rustraft.runtime_pressure.latency`, `rustraft.runtime_pressure.scale`,
`rustraft.runtime_pressure.pipeline`, and
`rustraft.runtime_pressure.read_backlog` records so log queries can group
rejected work by the exact pressure source.
`matrixraft_runtime_pressure_freshness_diagnostic_json_lines` emits
`rustraft.runtime_pressure.freshness` and per-issue freshness records, so
log-only release gates can reject stale, invalid, low-fresh, or future-dated
QPS, latency, and memory evidence without scraping Prometheus text.
`matrixraft_membership_readiness_diagnostic_json_lines` renders per-scope
failover, scale-up, and scale-down readiness decisions as structured JSON lines
so missing joint-consensus, learner catch-up, witness, and scheduler evidence is
visible in logs as well as Grafana.
`matrixraft_public_api_contract` groups these stable names into core type,
benchmark, observability, diagnostic, evidence-field, compatibility, RPC, and
safety surfaces so release reviews can see which APIs are intended for embedders
and dashboards.
`matrixraft_api_name_mappings` gives those reviews a deterministic name map from
canonical RustRaft API names to MatrixRaft facade names, TiKV/raft-rs concepts,
and ByteRaft/BaselineRaft reference vocabulary.
`matrixraft_public_api_contract_validation_prometheus` exports that validator as
release-dashboard metrics, including mapping readiness, coverage, unmapped
reference-required names, unmapped advertised names, per-category coverage, and
blockers.
Embedding examples are part of the same advertised contract: shipped examples
must also carry TiKV/raft-rs and ByteRaft/BaselineRaft mappings before the
public API validator accepts the surface.
`matrixraft_evidence_interface_names` does the same for production evidence
fields, including the per-peer packet-loss/reorder recovery counters and
fail-closed all-faulted-peers-recovered gate.
The public API validator also checks that every mapped canonical name appears in
one of those advertised surfaces, so a name map cannot silently drift away from
the supported API list.
Runtime queue surfaces are advertised in the same contract: `MailBox`,
`MailChannel`, `ChannelSelector`, and their checked send/fetch/select methods
map to TiKV raftstore-style mailboxes and BaselineRaft per-replica queues, so
production embedders can handle queue pressure and runtime lock failures without
turning them into process aborts. `MailChannel::try_send_many_checked` also
rejects oversized bursts before queueing them, which keeps per-peer fanout
memory bounded under release-scale workloads.
`matrixraft_reference_mapped_interface_names` is the fail-closed subset of that
surface: election RPCs, append/read/snapshot RPCs, storage/transport, WAL,
peer progress, runtime pressure, runtime-pressure freshness, scale-target
admission, peer-pipeline admission, benchmark runner, public API validation
Prometheus, snapshot lifecycle Prometheus, WAL lifecycle Prometheus, membership
readiness Prometheus, Grafana dashboard, alert rules, operator runbook names,
diagnostic-aware runbooks, runbook Prometheus, and provisioning runbook helpers
must all have reference mappings before the public API contract is considered
ready.
`PublicApiContractValidationReport::unmapped_reference_required_names` exposes
the exact missing names from that fail-closed subset so release gates can report
API mapping drift without parsing blocker strings.
`matrixraft_diagnostic_log_prometheus` exports those structured diagnostics as
severity and per-entry Prometheus counters for dashboards and alert triage.
`matrixraft_optimization_report` converts admin status evidence into deterministic
pipeline, WAL, quorum, and reorder-queue tuning hints for operators and CI.
`matrixraft_optimization_report_prometheus` renders those hints as Prometheus text
metrics, including readiness, per-hint, and component-level optimization series,
for Grafana dashboards and alerting rules.
`matrixraft_alert_rules_json` emits deterministic alert-rule metadata for
optimization readiness, critical optimization hints, fatal events, diagnostic
errors, and blocker presence, operator triage watch/attention states, critical
runbook steps, latency pressure, memory pressure, runtime-pressure freshness,
stale debug snapshots, debug-bundle validation failures, and provisioning validation failures, so
operators can wire the same metric contract into monitoring.
`matrixraft_observability_provisioning_json` emits the required metrics, Grafana
dashboard, alert rules, debug-bundle contract, and default operator runbook
steps needed to install the same monitoring contract in downstream runtimes.
`matrixraft_observability_required_metric_names` exposes the flattened metric
catalog for CI, Grafana provisioning, and release QPS/latency/memory parity gates.
`matrixraft_validate_required_metric_scrape_texts` checks actual Prometheus
scrape payloads against that required catalog plus the self-validation metrics,
and reports each missing metric by name before release evidence is trusted.
`matrixraft_debug_snapshot_json` emits a single timestamped support bundle
containing the admin report, diagnostic log entries, latency histograms, memory
gauges, scale counters, optimization hints, latency/memory/scale/optimization/runbook
and benchmark Prometheus text, Grafana dashboard metadata, alert-rule metadata, and an operator
triage summary with first action, top diagnostic, top alert, top optimization
hint, and runbook steps. Use `matrixraft_debug_snapshot_with_observability_metrics`
when a benchmark or runtime already has live latency, memory, and scale metrics;
use `matrixraft_debug_snapshot_with_runtime_metrics` when live latency and scale
metrics are available; use
`matrixraft_debug_snapshot_with_scale_metrics` when only live counters are
available; use `matrixraft_debug_snapshot_with_performance_targets` when
release-scale runs also have derived rates and explicit QPS/throughput targets
that should appear as optimization hints in the support bundle. The bundle includes a versioned
`matrixraft_debug_bundle_contract` marker and
`matrixraft_validate_debug_snapshot` or `matrixraft_validate_debug_snapshot_json`
for downstream tooling, including timestamp, diagnostic-log, optimization,
pointer, triage, Prometheus, Grafana, alert-rule, derived-field, and runbook consistency checks.
Use `matrixraft_debug_snapshot_with_runtime_pressure_evidence` when release
runs also need the fail-closed runtime-pressure admission decision, runtime
pressure Prometheus text, and `rustraft.runtime_pressure.*` diagnostic entries
inside the same support bundle.
Those runtime-pressure debug snapshots also carry freshness status, freshness
Prometheus text, and `rustraft.runtime_pressure.freshness` diagnostic entries,
so support bundles preserve the same QPS/latency/memory evidence-age gate used
by release dashboards.
Runtime-pressure metrics inside debug snapshots use exact Prometheus sample-name
and malformed-sample validation, so shadow metric names or corrupted labels
cannot satisfy the dashboard/debug-bundle contract.
The same exact sample-name contract is used for required optimization, latency,
memory, scale, scale-target, benchmark, diagnostic, and runbook metrics inside
`matrixraft_validate_debug_snapshot`.
Use `matrixraft_debug_snapshot_with_runtime_pressure_and_read_backlog_evidence`
for read-heavy release runs where pending ReadIndex and bounded-stale read
backlog must be included beside QPS, latency, memory, and peer-pipeline
pressure in the same support bundle.
`matrixraft_debug_snapshot_with_benchmark_scale_inputs` lets release tooling feed
the serializable benchmark-derived scale bundle directly into the debug snapshot
without manually unpacking rates and targets.
`matrixraft_debug_snapshot_with_benchmark_summary` attaches benchmark Prometheus
from a validated summary, while `matrixraft_debug_snapshot_with_benchmark_artifacts`
uses the full benchmark report plus summary so the same snapshot carries
BaselineRaft-derived scale targets, QPS/latency/throughput parity metrics, and
benchmark-parity runbook actions when a workload fails or drifts past threshold.
`matrixraft_production_readiness_report_with_benchmark_artifacts` keeps that
diagnostic behavior for release triage, while
`matrixraft_production_readiness_report_with_asserted_benchmark_artifacts` is
the fail-closed release-gate helper for callers that require the matched report
and summary to prove clean QPS, latency, throughput, correctness, CPU, and
memory parity before a readiness report is built.
CPU and peak resident-memory parity require complete baseline/RustRaft sample
pairs; one-sided resource evidence is rejected instead of being treated as an
absent optional metric.
Benchmark report, summary, and runtime-pressure artifact schemas use the
`matrixraft.*` namespace; scrape metric names remain `rustraft_*` for existing
Grafana and alert compatibility.
`matrixraft_debug_snapshot_with_benchmark_runtime_pressure_artifacts` extends
that release bundle with runtime-pressure admission, Prometheus, and diagnostic
log evidence derived from the same BaselineRaft-backed scale targets.
`matrixraft_debug_snapshot_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
adds pending ReadIndex and bounded-stale read backlog pressure to that same
benchmark-derived support bundle.
`matrixraft_debug_snapshot_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
adds node-runtime timer utilization to the same QPS, pipeline, and read-backlog
support bundle.
`matrixraft_debug_snapshot_metadata_prometheus` exports the debug snapshot
generation timestamp and age as Prometheus gauges for Grafana freshness triage.
`matrixraft_debug_bundle_validation_prometheus` turns that validation report into
Prometheus readiness, issue-count, first-issue, and per-issue series for
dashboards and alerting.
`matrixraft_operator_runbook_prometheus` exports grouped remediation counts,
per-step presence, and the first active runbook step for Grafana triage.
`matrixraft_observability_provisioning_validation_prometheus` exposes the same
readiness, issue-count, first-issue, and per-issue shape for provisioning drift.

Production readiness also requires real BaselineRaft benchmark evidence. Model
benchmark runners remain available for unit tests, but
`matrixraft_production_readiness_report()` blocks production claims unless
benchmark evidence proves the RustRaft runtime ran the same release-scale workload dimensions and passed
correctness plus the configured latency and throughput threshold, while the
benchmark summary preserves optional CPU and peak resident-memory ratios for
resource-parity triage. It also
exposes `matrixraft_scale_rate_metrics_from_benchmark_report` and
`matrixraft_scale_optimization_targets_from_baseline_raft_report` so the same
BaselineRaft parity report can feed release-scale QPS/throughput optimization
hints and `matrixraft_debug_snapshot_with_performance_targets`. Use
`matrixraft_scale_optimization_inputs_from_benchmark_report` when release tools
want the derived RustRaft rates, BaselineRaft-backed targets, and resulting
optimization hints as one serializable bundle.
Use `matrixraft_production_readiness_input_with_runtime_pressure_evidence` when
those rates and targets should gate production readiness together with current
memory, p99 latency, and per-peer pipeline pressure.
Use `matrixraft_production_readiness_input_with_benchmark_runtime_pressure_artifacts`
to validate the benchmark artifacts and attach that same runtime-pressure gate
to `ProductionReadinessInput` in one release-automation call.
Use
`matrixraft_production_readiness_input_with_asserted_benchmark_runtime_pressure_artifacts`
when that release-automation call should fail before deriving QPS targets from
matched but non-production-clean benchmark artifacts.
Use
`matrixraft_production_readiness_input_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
for read-heavy releases where pending ReadIndex or bounded-stale backlog must
participate in that gate.
Use the asserted read-backlog variant when failing benchmark artifacts should
stop that path before backlog metrics are trusted.
Use
`matrixraft_production_readiness_input_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
for release automation that must include QPS targets, peer pipeline, read
backlog, and node-runtime timer pressure in one admission decision.
Use the asserted complete-pressure variant when failing benchmark artifacts
should stop that path before read-backlog or timer evidence is attached.
Use `matrixraft_production_readiness_report_with_benchmark_runtime_pressure_artifacts`
for the one-call CI gate that turns those validated artifacts and live pressure
signals into a `ProductionReadinessReport`.
Use
`matrixraft_production_readiness_report_with_asserted_benchmark_runtime_pressure_artifacts`
for the stricter CI gate that requires production-clean benchmark artifacts
before runtime-pressure evidence is attached.
Use
`matrixraft_production_readiness_report_with_benchmark_runtime_pressure_and_read_backlog_artifacts`
when the same CI gate must fail closed on live read backlog pressure too.
Use the asserted read-backlog report variant when CI must also reject failing
benchmark artifacts before building that report.
Use
`matrixraft_production_readiness_report_with_benchmark_runtime_pressure_read_backlog_and_node_runtime_timer_artifacts`
when that CI gate must also fail closed on timer-queue saturation.
Use the asserted complete-pressure report variant when CI must prove benchmark
parity before combining read-backlog and timer-pressure evidence.
Use `matrixraft_release_benchmark_runtime_timer_status` in release benchmark
producer and verifier paths when no timer pressure is observed yet; this keeps
the serialized readiness artifact and verifier on the same scheduler/timer
baseline instead of relying on duplicate example-local fixtures.
Use `matrixraft_benchmark_runtime_pressure_readiness_artifact` when CI should
emit the readiness report, readiness Prometheus scrape text, runtime-pressure
Prometheus scrape text, and structured diagnostic log lines as one serialized
release artifact.
Use `matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact` for
promotion and release gates that must reject matched-but-failing benchmark
artifacts before any Prometheus, Grafana, or diagnostic payload is emitted.
Use `matrixraft_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
when the serialized release artifact must bind benchmark parity, runtime
pressure, and read backlog evidence together.
Use
`matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
when read-heavy release gates must also prove that the benchmark artifacts were
production-clean before pending-read evidence is trusted.
Use `matrixraft_validate_benchmark_runtime_pressure_readiness_artifact` on the
consumer side before promoting a release from that artifact, so benchmark,
runtime-pressure, structured-label, readiness-metric, runtime-pressure-metric,
and JSON-log evidence stay bound to the same inputs.
Use `matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact`
when the consumer must recompute the artifact through the same
production-clean benchmark gate used by the producer.
Use
`matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
when that artifact includes live pending ReadIndex or bounded-stale read backlog
evidence.
Use
`matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog`
for read-backlog release artifacts whose source benchmark evidence must be
revalidated as production-clean.
Use
`matrixraft_validate_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
when promotion must recompute benchmark parity, read backlog, and timer-pressure
evidence from the same release inputs.
Use
`matrixraft_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
and
`matrixraft_validate_asserted_benchmark_runtime_pressure_readiness_artifact_with_read_backlog_and_node_runtime_timer`
for the strongest release gate: clean benchmark artifacts, read backlog,
peer-pipeline pressure, node-runtime timer pressure, Prometheus, and diagnostic
logs all stay bound to one input set.
Production readiness also
requires `matrixraft_fault_harness_readiness_report`
evidence for the BaselineRaft-derived packet-loss/partition-heal, slow WAL fsync,
snapshot-during-membership-change, leader-transfer-under-load,
follower-rejoin-after-compaction, and rolling-restart joint-consensus scenarios.
Each required fault scenario must come from distinct real processes with
independent WAL/snapshot directories, a non-trivial runtime, observed client
operations, injected fault events, safety/recovery checks, metrics, and a report
path.

## Why It Lives Separately

Keeping MatrixRaft in a separate repository gives TemporalStore a stable
consensus-readiness boundary:

- TemporalStore can consume a pinned MatrixRaft revision.
- Future RustRaft state-machine, transport, snapshot, and membership traits can
  be added without burying them inside the TemporalStore application crate.
- Shared tests can validate the contract independently from production storage
  process wiring.

## Current Scope

This standalone version is a clean separation of the RustRaft layer from the store engine: RustRaft owns the reusable Raft-facing contracts and model
primitives, while TemporalStore owns only FSM/domain adapters, codecs, process
wiring, and storage-engine integration. RustRaft now owns the stable
node/options, storage, transport, status, metric, safety-policy, WAL record,
snapshot fence, membership, and BaselineRaft-parity surfaces that TemporalStore can
consume from data-node and metaserver code.

Read safety now includes structured report types for quorum, applied-index
fences, leader lease-read eligibility, and bounded-stale follower reads. These
reports are intended to be filled with observed process-path evidence before a
TemporalStore deployment claims BaselineRaft-style read-index or lease-read parity.

Transport contracts include fail-fast request/response validators and a generic
in-memory transport router. The router is meant for library tests and harness
adapters; production TemporalStore still owns real process transports and
durable FSM adapters.

The `matrixraft_temporalstore_extraction_plan()` API is the typed migration
ledger. It records which Raft responsibilities are already owned by this
standalone crate, which remain pending migration, and which must stay as
TemporalStore-specific adapters.

## Open Source Surface

RustRaft exposes its standalone boundary through public modules for `node`,
`cluster`, `membership`, `wal`, `snapshot`, `transport`, `status`, `metrics`,
`readiness`, `storage`, `benchmark`, and `fault`. The
`matrixraft_open_source_surface()` report names those modules, embedding examples,
BaselineRaft parity matrix entries, benchmark harness APIs, and compatibility
reports so consumers can check the published surface without scraping docs.

RustRaft owns generic Raft contracts, parity/readiness reports, benchmark
interfaces, transport/storage/state-machine traits, and status/metrics surfaces.
TemporalStore keeps adapter docs and implementation details for command codecs,
TemporalEngine apply logic, metaserver scheduling, HTTP/process endpoints, and
storage-object wiring.

`matrixraft_standalone_readiness_report()` is the fail-closed status check for a
non-TemporalStore embedding. It only reports `ProductionReady` when the public
crate surface covers node lifecycle, replication, election/pre-vote, membership,
WAL recovery, snapshots, read-index/lease-read, and status/metrics/readiness
without relying on TemporalStore adapter code.

`tests/standalone_embedding_contract.rs` repeats that status check as five
executable embedding passes: node lifecycle, replication/read safety,
membership workflow, WAL/snapshot durability, and final readiness/API coverage.
Those tests are the guardrail for continuing to move generic Raft substrate out
of TemporalStore and into this standalone crate.

The intended TemporalStore adapter shape is:

```rust
struct TemporalRaftConsensusBackend {
    node: rustraft::node::NodeRuntime<TemporalStoreStateMachine, TemporalTransport>,
    codec: TemporalCommandCodec,
    engine: TemporalEngine,
}
```

`matrixraft_temporalstore_adapter_shape()` exposes this as a typed compatibility
report. RustRaft owns consensus behavior inside the node runtime; TemporalStore
owns command encoding, apply semantics, storage engine integration, and
process/admin surfaces.

The fault-harness API names the BaselineRaft-derived process scenarios that
TemporalStore must prove with spawned data-node and metaserver processes:
packet loss, slow WAL fsync, snapshot during membership change, leader transfer
under load, follower rejoin after compacted logs, and rolling restart with
pending joint consensus.
Each required scenario must include at least three spawned processes, at least
three distinct observed process IDs, independent WAL/snapshot stores, and a
scenario-specific report path before production readiness can pass.

## Test

```bash
cargo test
```

Run the five-pass standalone embedding contract:

```bash
cargo test --test standalone_embedding_contract
```

## Examples

The examples are intentionally storage/runtime agnostic. They show how an
application such as TemporalStore should feed process evidence into the
standalone RustRaft contract.

Build and run the readiness report example:

```bash
cargo run --example readiness_report
```

Run the read-safety policy example:

```bash
cargo run --example read_safety
```

Inspect the open-source embedding surface:

```bash
cargo run --example open_source_surface
```

Build a BaselineRaft-style operational evidence bundle:

```bash
cargo run --example baseline_raft_operational_evidence
```

This example validates and prints a
`rustraft.baseline_raft_operational_evidence_bundle.v1` JSON document. Real
embedders should replace the example counters with observations from spawned
data-node and metaserver processes, then call
`matrixraft_validate_baseline_raft_operational_evidence_bundle` before forwarding the
bundle into service readiness or CI gates. The bundle deliberately keeps the
five BaselineRaft-derived evidence families separate:

- read-index and lease-read safety
- learner, witness, leader-transfer, and joint-consensus membership semantics
- per-peer replication pipeline and reorder-queue pressure
- snapshot sender/downloader, retry, timeout, rollback, and compacted-log rejoin
- WAL segment lifecycle, retained ranges, compaction, and slow-fsync pressure

Run the standalone BaselineRaft-vs-RustRaft benchmark script from the RustRaft repo:

```bash
BASELINE_RAFT_ROOT=/path/to/baseline_raft \
  RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT=target/baseline_raft-vs-rustraft-benchmark/pressure-snapshot.json \
  RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_MEMORY_BYTES=0 \
  RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_BYTES=0 \
  RUSTRAFT_BENCHMARK_APPEND_P99_MS=0 \
  RUSTRAFT_BENCHMARK_READ_INDEX_P99_MS=0 \
  RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_REQUESTS=0 \
  RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_PENDING_TICKS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_MAX_PENDING_TICKS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_REJECTED_TICKS=0 \
  bash scripts/baseline_raft_vs_rustraft_benchmark.sh \
  --release \
  --node-count 5 \
  --iterations 128 \
  --batch-size 16 \
  --payload-size-bytes 4096 \
  --pass-tolerance-percent 10.0 \
  --out target/baseline_raft-vs-rustraft-benchmark/report.json \
  --summary-out target/baseline_raft-vs-rustraft-benchmark/summary.json \
  --scale-out target/baseline_raft-vs-rustraft-benchmark/scale.json \
  --readiness-input target/baseline_raft-vs-rustraft-benchmark/readiness-input.json \
  --readiness-artifact-out target/baseline_raft-vs-rustraft-benchmark/readiness-artifact.json \
  --readiness-labels service=rustraft-ci,workload=release-scale
```

Release jobs can generate the pressure snapshot consumed by the benchmark and
verifier from the same live counters:

```bash
RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT_OUT=target/baseline_raft-vs-rustraft-benchmark/pressure-snapshot.json \
  RUSTRAFT_BENCHMARK_PROCESS_RESIDENT_MEMORY_BYTES=0 \
  RUSTRAFT_BENCHMARK_HEAP_ALLOCATED_BYTES=0 \
  RUSTRAFT_BENCHMARK_APPEND_P99_MS=0 \
  RUSTRAFT_BENCHMARK_READ_INDEX_P99_MS=0 \
  RUSTRAFT_BENCHMARK_PENDING_READ_INDEX_REQUESTS=0 \
  RUSTRAFT_BENCHMARK_PENDING_BOUNDED_STALE_READS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_PENDING_TICKS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_MAX_PENDING_TICKS=0 \
  RUSTRAFT_BENCHMARK_RUNTIME_TIMER_REJECTED_TICKS=0 \
  cargo run --release --example baseline_raft_pressure_snapshot -- \
  --out target/baseline_raft-vs-rustraft-benchmark/pressure-snapshot.json
```

The script does not enter or depend on the TemporalStore checkout. It fails
closed with `benchmark:real_baseline_raft_missing` unless `BASELINE_RAFT_ROOT` contains a
`baseline_raft_parity_benchmark` harness or `BASELINE_RAFT_BENCHMARK_BIN` points to one.
The model runner is intentionally not used for production parity.
Production parity uses release-mode same-workload evidence, requires explicit
reference and Rust implementation identity in the benchmark
artifacts, emits benchmark-derived scale optimization inputs for release
QPS/latency gates, can emit the combined asserted benchmark/runtime-pressure,
read-backlog, and node-runtime timer readiness artifact for release promotion,
and fails closed before writing that artifact unless the benchmark evidence is
production-clean:
For real release jobs, set the `RUSTRAFT_BENCHMARK_PROCESS_*`,
`RUSTRAFT_BENCHMARK_HEAP_*`, `RUSTRAFT_BENCHMARK_*_P99_MS`,
`RUSTRAFT_BENCHMARK_PENDING_*`, and `RUSTRAFT_BENCHMARK_RUNTIME_TIMER_*`
environment variables from live memory, latency, read-backlog, and node-runtime
timer counters before emitting the readiness artifact, or provide
`RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT` with a JSON object containing any of
`memory_metrics`, `memory_thresholds`, `latency_metrics`,
`latency_thresholds`, `read_backlog_metrics`, `read_backlog_thresholds`,
`timer_status`, and `timer_thresholds`. The
`baseline_raft_pressure_snapshot` example writes that JSON atomically from the
same `RUSTRAFT_BENCHMARK_*` live counter environment, so release jobs can save a
single replayable pressure artifact before the benchmark begins. The verifier
reuses the same snapshot or environment and accepts explicit flags for
backlog/timer replay, so artifact rechecks fail closed when the supplied
pressure snapshot does not match the saved readiness artifact.
`matrixraft_baseline_raft_benchmark_summary_prometheus` exports the same summary
as `rustraft_baseline_raft_benchmark_*` gauges, including worst p50/p99 latency
ratios, worst throughput/CPU/peak-memory ratios, failed workload count,
artifact generation time, age, remaining freshness, freshness status, the hard
freshness flag used by `RustRaftBaselineRaftBenchmarkFreshnessLost`, and
per-workload p50/p99/throughput/CPU/peak-memory ratios for release dashboards.
The default Grafana and alert provisioning includes those benchmark
parity/freshness panels and ratio-regression alerts so
QPS/latency/CPU/memory parity failures are visible beside runtime pressure.
`RustRaftBaselineRaftBenchmarkResourceRegression` also alerts on CPU and peak
resident-memory ratios so resource regressions are visible even when p50/p99
latency and throughput ratios still pass.
`RustRaftBaselineRaftBenchmarkFreshnessLost` routes to
`refresh_baseline_raft_benchmark_evidence`, which requires rerunning
release-mode BaselineRaft-vs-RustRaft parity and proving the benchmark
freshness/status/timestamp metrics are current before QPS, latency, CPU, or
memory claims are accepted.

- at least 5 nodes
- at least 128 iterations per workload
- batched workloads must use batch size 2 or larger
- payloads must be at least 4096 bytes
- pass tolerance must be finite and no higher than 10%

Use `--node-count 9` for the larger scale run. The script writes both the full
report and compact summary, then verifies that the two artifacts match, are fresh,
use release-mode evidence, cover every required workload, and satisfy the
production parity gate. Saved artifacts can be rechecked later:

```bash
bash scripts/verify_baseline_raft_benchmark_artifacts.sh \
  --release \
  --report target/baseline_raft-vs-rustraft-benchmark/report.json \
  --summary target/baseline_raft-vs-rustraft-benchmark/summary.json \
  --scale target/baseline_raft-vs-rustraft-benchmark/scale.json \
  --readiness-input target/baseline_raft-vs-rustraft-benchmark/readiness-input.json \
  --readiness-artifact target/baseline_raft-vs-rustraft-benchmark/readiness-artifact.json \
  --readiness-labels service=rustraft-ci,workload=release-scale \
  --pressure-snapshot target/baseline_raft-vs-rustraft-benchmark/pressure-snapshot.json \
  --pending-read-index-requests 0 \
  --pending-bounded-stale-reads 0 \
  --runtime-timer-pending-ticks 0 \
  --runtime-timer-max-pending-ticks 0 \
  --runtime-timer-rejected-ticks 0
```

When the scale path is supplied, the verifier recomputes the derived QPS and
throughput scale optimization inputs and rejects scale-rate, scale-target, or
hint drift; every release-scale QPS and throughput target must be non-zero, so
an empty or malformed BaselineRaft sample cannot silently become a 0 percent
production target. When the readiness paths are supplied, it also recomputes the
combined asserted benchmark/runtime-pressure/read-backlog/node-runtime-timer
readiness artifact through the production-clean benchmark gate and rejects
readiness-report, Prometheus, diagnostic-log, or label drift before release
automation trusts the artifacts.

BaselineRaft's native `example/kv/kv_benchmark.cc` / `kvbench` is detected and
reported as partial evidence, but it does not replace the required JSON parity
harness because it does not cover every production workload.
To generate a normal failing report through that partial path, pass
`--native-kvbench-adapter` after building BaselineRaft's example `kvserver` and
`kvbench` binaries:

```bash
BASELINE_RAFT_ROOT=/path/to/baseline_raft \
  bash scripts/baseline_raft_vs_rustraft_benchmark.sh \
  --native-kvbench-adapter \
  --out target/baseline_raft-vs-rustraft-benchmark/native-kvbench-report.json
```

These examples are also covered by integration tests so the public snippets stay
in sync with the crate API.

## Contributing

Contributions are welcome — please read [`CONTRIBUTING.md`](CONTRIBUTING.md) and the
[`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md). One rule matters most for this crate:
never weaken a safety predicate to make a test pass. See
[`docs/read_index_safety_review.md`](docs/read_index_safety_review.md) for the
read-index safety model and the invariants that must never regress.

The Minimum Supported Rust Version (MSRV) is **1.82**.

## Security

To report a vulnerability, follow [`SECURITY.md`](SECURITY.md) (private disclosure
via GitHub Security Advisories). Consensus-safety defects — stale reads,
split-brain, or lost writes — are treated as security issues.

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md).

## License

Licensed under the Apache License, Version 2.0 ([`LICENSE`](LICENSE)). Unless you
explicitly state otherwise, any contribution intentionally submitted for inclusion
in this project shall be licensed as above, without any additional terms or
conditions.

Third-party dependency licenses and attributions are listed in
[`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md).

## Trademarks

Product and crate names are trademarks of MatrixArkAI; see
[`TRADEMARKS.md`](TRADEMARKS.md).
