# MatrixRaft

[![CI](https://github.com/matrixarkai/matrixraft/actions/workflows/ci.yml/badge.svg)](https://github.com/matrixarkai/matrixraft/actions/workflows/ci.yml)
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
- `matrixraft_data_node_process_rollout_readiness_report`
- `matrixraft_meta_process_rollout_readiness_report`
- `matrixraft_baseline_raft_runtime_capability_report`
- `matrixraft_baseline_raft_runtime_capability_prometheus`
- `matrixraft_public_api_contract`
- `matrixraft_open_source_surface`
- `matrixraft_temporalstore_adapter_shape`
- `matrixraft_temporalstore_extraction_plan`
- `matrixraft_metric_names`
- `matrixraft_grafana_dashboard`
- `matrixraft_grafana_dashboard_json`
- `matrixraft_alert_rules`
- `matrixraft_alert_rules_json`
- `matrixraft_diagnostic_log_prometheus`
- `matrixraft_observability_provisioning`
- `matrixraft_observability_provisioning_json`
- `matrixraft_observability_provisioning_runbook_steps`
- `matrixraft_observability_provisioning_validation_prometheus`
- `matrixraft_validate_observability_provisioning`
- `matrixraft_validate_observability_provisioning_json`
- `matrixraft_operator_triage_summary`
- `matrixraft_operator_triage_prometheus`
- `matrixraft_operator_runbook_steps`
- `matrixraft_operator_runbook_prometheus`
- `matrixraft_debug_bundle_contract`
- `matrixraft_validate_debug_snapshot`
- `matrixraft_validate_debug_snapshot_json`
- `matrixraft_debug_bundle_validation_prometheus`
- `matrixraft_admin_diagnostic_log_entries`
- `matrixraft_admin_diagnostic_json_lines`
- `matrixraft_optimization_report`
- `matrixraft_optimization_report_prometheus`
- `matrixraft_debug_snapshot_json`
- `matrixraft_debug_snapshot_metadata_prometheus`

Run `cargo run --example debug_artifacts` to print a complete sample support
artifact with the debug snapshot, JSON log lines, diagnostic and optimization
Prometheus text, runbook Prometheus text, Grafana dashboard JSON, alert-rule
JSON, validation report, and validation Prometheus series in one envelope.

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

`matrixraft_production_readiness_report` is the fail-closed deployment gate. It
wraps the semantic parity report with runtime evidence for peer pipeline,
snapshot lifecycle, WAL lifecycle, data-node rollout, metaserver rollout,
admin/status observability, fault harness results, and real BaselineRaft benchmark
parity.
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
sender ack completion and downloader install completion, not only partial
progress counters.
Membership evidence must prove joint-consensus commits with both old and new
quorum acknowledgements for voter-changing scale-up and scale-down transitions.
WAL lifecycle evidence must prove segment compaction after slow-fsync pressure,
not only slow-fsync and released-segment counters observed independently.
`matrixraft_baseline_raft_runtime_capability_prometheus` renders that report as generic
`rustraft_baseline_raft_*` Prometheus text metrics. Product runtimes such as
TemporalStore can attach their own service labels without duplicating the
capability-matrix logic.
`matrixraft_grafana_dashboard_json` renders a deterministic Grafana dashboard
contract for the canonical `rustraft_*` readiness, latency, queue, WAL, snapshot,
blocker, and fatal-event metrics.
`matrixraft_admin_diagnostic_json_lines` renders the admin report as structured
JSON log lines for embedders that want RustRaft-owned debugging context without
adopting a specific logging crate.
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
runbook steps, stale debug snapshots, debug-bundle validation failures, and
provisioning validation failures, so operators can wire the same metric contract
into monitoring.
`matrixraft_observability_provisioning_json` emits the required metrics, Grafana
dashboard, alert rules, debug-bundle contract, and default operator runbook
steps needed to install the same monitoring contract in downstream runtimes.
`matrixraft_debug_snapshot_json` emits a single timestamped support bundle
containing the admin report, diagnostic log entries, optimization hints,
optimization Prometheus text, runbook Prometheus text, Grafana dashboard
metadata, alert-rule metadata, and an operator triage summary with first action,
top diagnostic, top alert, top optimization hint, and runbook steps. The bundle includes a
versioned `matrixraft_debug_bundle_contract` marker and
`matrixraft_validate_debug_snapshot` or `matrixraft_validate_debug_snapshot_json`
for downstream tooling, including timestamp, diagnostic-log, optimization,
pointer, triage, Prometheus, Grafana, alert-rule, derived-field, and runbook consistency checks.
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
benchmark evidence proves the Rust RustRaft runtime ran the same release-scale workload dimensions and passed
correctness plus the configured latency and throughput threshold. It also
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

## Multiple Raft Groups In One Process

`MatrixRaftMultiRaftServer` hosts many groups at once. It keys its nodes by
`MatrixRaftRouteKey { group_id, node_id }`, so a store that owns a replica of
each of many groups registers one entry per replica:

```rust
use matrixraft::{MatrixRaftGroupContextBuilder, MatrixRaftMultiRaftServer};

let context = MatrixRaftGroupContextBuilder::new().transport(transport).build()?;
let mut server = MatrixRaftMultiRaftServer::new(context);

server.create_node(options_for_group_7, 0)?;
server.create_node(options_for_group_8, 0)?;

server.group_count();   // 2
server.group_ids();     // [7, 8]
server.route_keys();    // [(7, node), (8, node)]
```

Creation and removal have batch forms (`create_nodes`, `unregister_groups`),
planning forms (`plan_create_nodes`, `plan_unregister_group`) that return what a
batch *would* do without doing it, and `*_best_effort` variants that report a
per-group outcome instead of failing the whole batch. Most inspection is
available per group or per route key — `node_ids_on_group`, `group_topology`,
`counts_by_group`, `runtime_wiring`.

### What one more group costs

Each registered node owns a `NodeRuntime`, and a `NodeRuntime` is an OS thread
plus a command channel. Measured with `examples/group_scaling.rs` on Linux,
release build, one group count per process:

| groups | resident delta | per group | threads |
|---|---|---|---|
| 1 | 0.3 MiB | 272 KiB | 1 |
| 64 | 3.4 MiB | 54 KiB | 64 |
| 256 | 11.7 MiB | 47 KiB | 256 |
| 1024 | 39.3 MiB | 39 KiB | 1024 |

One thread per group at every size, and per-group memory settling near 39 KiB —
the larger figures at small counts are fixed startup cost divided by a small
denominator, not a group being more expensive. Plan accordingly: ten thousand
groups in one process is ten thousand threads, and the memory is the smaller
part of that bill.

Run it on your own hardware rather than taking these numbers:

```bash
cargo run --release --example group_scaling -- 1024
```

### One setting the server does apply: the tick

`MatrixRaftGroupContextBuilder::tick_interval` sets a heartbeat interval for the
whole server, and a group created through the server that leaves
`tick_interval_ms` at zero takes it:

```rust
let context = MatrixRaftGroupContextBuilder::new()
    .transport(transport)
    .tick_interval(250)
    .build()?;
let mut server = MatrixRaftMultiRaftServer::new(context);

options.tick_interval_ms = 0;     // says nothing, so it inherits 250ms
server.create_node(options, 0)?;
```

A group that sets its own interval keeps it — the server's value is a default,
not an override. Ask a running group what it ended up with through
`server.node(group_id, node_id)?.runtime_status()?.timer_status`.

Zero used to mean something worse than "unset": `to_raft_config` applies
`tick_interval_ms.max(1)`, so a group that named no tick ran a **1 ms**
heartbeat with an election timeout of `election_cycle_tick` milliseconds.

### Sharing threads between groups: the driver

`NodeRuntime` gives every group a thread, so the table above is 1.00 threads per
group. `Driver` and `DriverWorkerPool` are the other shape: a fixed set of
threads that many groups share.

```rust
let options = DriverOptions { worker_num: 4, max_messages_each_poll: 64,
                              max_queue_depth: 4096, tick_interval_ms: 10,
                              ..DriverOptions::default() };

// Ticks: one thread, a due-time heap, any number of groups.
let driver = Driver::start(options)?;
driver.register_group(DriverGroupKey::new(group_id, node_id), tick_receiver)?;
driver.register_group_every(key, receiver, 250)?;  // or an interval of its own

// Mail: worker_num threads selecting across every group's channel.
let pool: DriverWorkerPool<MyMail> = DriverWorkerPool::start(options)?;
pool.register_group(key, handler)?;
pool.send(key, MailPriority::Normal, mail)?;       // refused at max_queue_depth
```

Each group keeps its own tick interval, and a group's mail arrives in batches of up to
`max_messages_each_poll` rather than one call per message. A group whose handler
is busy is refused new mail at `max_queue_depth` instead of queueing without
bound.

`driver.thread_count()` is `worker_num + 1` and `pool.thread_count()` is
`worker_num`, at one group or at ten thousand. Measured with
`examples/driver_scaling.rs` against `examples/group_scaling.rs`, one group count
per process, release build, four workers:

| groups | a runtime per group | | sharing a driver | |
|---|---|---|---|---|
| | threads | per group | threads | per group |
| 64 | 64 | 57 KiB | **6** | 2.0 KiB |
| 256 | 256 | 47 KiB | **6** | 1.0 KiB |
| 1024 | 1024 | 39 KiB | **6** | 612 B |
| 4096 | — | — | **6** | 563 B |
| 10000 | — | — | **10** (8 workers) | 560 B |

At 1024 groups that is 1024 threads against 6, and 39 KiB per group against
about 640 B — the memory figure held between 612 and 676 B over five runs, and
10000 groups reported 560 B in three runs out of three. Registering a group
costs about 800 ns rather than about 113 µs, because no thread is spawned.

The probe sends every group a message and refuses to report until all of them
have received it, so a registry that held groups and drove nothing would fail
rather than look thrifty.

The driver is a component to build a host on; it does not replace `NodeRuntime`,
and nothing in this crate wires the two together yet.

### Applying, and a read that waits for its group

Apply is the slow half of a group: it touches the state machine and the disk.
`Applier` runs every registered group's apply work on the same pool —
`applier_num` threads, batches bounded by `apply_max_batch_count`.

```rust
let applier = Applier::start(ApplierOptions { applier_num: 4,
                                              apply_max_batch_count: 64,
                                              ..ApplierOptions::default() })?;
applier.register_group(key, handler)?;          // handler: impl ApplyHandler
applier.submit(key, apply_task)?;               // the crate's own ApplyTask
applier.progress(key);                          // applied_index, batches, tasks
```

`ApplyHandler::apply` returns the index the group reached, so the applier owns
the scheduling and the waiting and nothing about the state machine. The applied
index never moves backwards: a handler reporting an older index than the group
already reached does not un-apply anything.

**A read can wait for its group.** `docs/read_index_safety_review.md` notes that
a follower read is served only when `applied_index >= read_index` and that
apply-wait was a future enhancement. This is it:

```rust
match applier.wait_for_applied(key, read_index, Duration::from_millis(50))? {
    ApplyWait::Reached { .. }   => serve_the_read(),
    ApplyWait::TimedOut { .. }  => report_follower_apply_pending(),
    ApplyWait::GroupGone { .. } => report_not_leader(),   // retrying will not help
}
```

`Reached` is never reported early — the index it carries is read under the same
lock the applier advances it with — and a cancelled group wakes its waiters
rather than leaving them until the timeout.

### A configured transfer limit that actually limits

`SnapshotLifecycle::poll_send_requests_with_limiter` has always taken a
`RateLimiter`, and `MatrixRaftRateLimiterConfig` has always recorded what a host
wanted — on the group context, the node creator and the runtime wiring, with
setters for both directions. Nothing joined them: the configured number reached
no limiter.

```rust
let config = MatrixRaftRateLimiterConfig { bytes_limit_per_sec: 8 << 20,
                                           check_cycle_sec: 1 };
let limiter = Arc::new(Mutex::new(config.to_byte_quota_limiter()?));

// A limiter hands bytes out and never gets them back on its own, so the
// driver's tick puts the quota back every check cycle.
let refiller = RateLimiterRefiller::new(Arc::clone(&limiter));
driver.register_group_every(key, refiller, config.check_cycle_sec as u64 * 1000)?;

lifecycle.poll_send_requests_with_limiter(&mut limiter.lock()?)?;
```

A group's own configuration is reachable from what it was built with:
`wiring.snapshot_send_limiter()?` and `wiring.snapshot_download_limiter()?`
return the limiters that group was configured with, or `None`.

A zero in either field is refused rather than accepted. A limiter built from a
zero grants nothing and stops every transfer it gates, which is not a state to
reach by leaving a field at its default — leave the whole limiter unset to mean
no limit.

### Snapshot work: four jobs, four pools

Moving a snapshot is four jobs, not one. Creating reads the state machine and
writes a checkpoint; sending pushes chunks at a peer that asked; downloading
pulls them from a peer that has them; loading installs what arrived. They cost
different things and they stall for different reasons, which is why there are
four counts rather than one.

```rust
let pools: SnapshotPools<MyWork> = SnapshotPools::start(SnapshotPoolOptions {
    snapshot_creator_num: 1,
    snapshot_sender_num: 2,
    snapshot_downloader_num: 2,
    snapshot_loader_num: 1,
    max_queue_depth: 256,
})?;
pools.register_group(key, handler)?;                       // all four phases
pools.submit(key, SnapshotPhase::Create, work)?;
pools.stats(SnapshotPhase::Send);                          // per phase
```

`thread_count()` is the four counts added up — ten above — at one group or at
sixty-four. Registration is all-or-nothing across the four, and a cancel stops
all four.

The property that earns the separation: **a stalled phase does not stop the
others.** Its test parks the single send thread on a peer that never reads, then
requires twenty checkpoints to finish anyway. Collapse the four pools into one
and that test fails, which is the whole argument for four counts in one
assertion.

### Settings this crate still only records

`MatrixRaftGroupContext` and `MatrixRaftRuntimeWiring` carry a little more
configuration a host store would use: `reader_num`, `executor_num`,
`watched_address_resolver` and `store_id`.

Those are recorded, planned over and reported on. Of them only `flexible_apply`
reaches an implementation (in `fsm`).

`driver_batch_bytes` is the driver's, and it applies when a host says how big a
mail is:

```rust
let pool = DriverWorkerPool::start_with_mail_size(
    options,                                   // driver_batch_bytes: 64 * 1024
    Arc::new(|mail: &MyMail| mail.encoded_len()),
)?;
```

`start` without one bounds a batch by `max_messages_each_poll` alone, because
the pool cannot measure a `Mail` it knows nothing about. A mail larger than the
whole budget is still delivered, on its own, rather than stranded.

### Heartbeats from many groups to one peer, in one batch

`HeartbeatMerger` buckets by destination address rather than by group, which is
the shape that lets a store hosting many groups send one batch to a shared peer
instead of one message per group. What it lacked was something to decide *when*
to let the buckets go — which is what `merge_heartbeat_interval_milli` names,
and what the driver's tick now spends:

```rust
let flusher = HeartbeatFlusher::new(HeartbeatMerger::enabled(), sender);
driver.register_group_every(key, flusher.clone(), merge_heartbeat_interval_milli)?;

// on the send path
match flusher.maybe_merge(message, &resolver)? {
    None => {}                       // absorbed; it goes with the next flush
    Some(message) => transport.send(message)?,   // not a heartbeat, or merging is off
}
```

Measured by its test: 64 groups heartbeating 4 peers queue 256 messages and
flush **4 batches**, one per address, carrying all 256. A merger that is
disabled absorbs nothing and hands every message straight back, so the call can
stay in place and the setting turns the behaviour off.

An `AppendEntries` carrying entries is replication, not a heartbeat, and is
never held back.

So treat the group context as a place to record the wiring a host should
implement, not as a description of what happens when you set it. The benchmark
harness reflects the same boundary: `BenchmarkOptions` takes a node count and no
group count, and its workloads run as a single group.

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
  bash scripts/baseline_raft_vs_rustraft_benchmark.sh \
  --release \
  --node-count 5 \
  --iterations 128 \
  --batch-size 16 \
  --payload-size-bytes 4096 \
  --pass-tolerance-percent 10.0 \
  --out target/baseline_raft-vs-rustraft-benchmark/report.json \
  --summary-out target/baseline_raft-vs-rustraft-benchmark/summary.json
```

The script does not enter or depend on the TemporalStore checkout. It fails
closed with `benchmark:real_baseline_raft_missing` unless `BASELINE_RAFT_ROOT` contains a
`baseline_raft_parity_benchmark` harness or `BASELINE_RAFT_BENCHMARK_BIN` points to one.
The model runner is intentionally not used for production parity.
Production parity uses release-mode same-workload evidence, requires explicit
reference and Rust implementation identity in the benchmark
artifacts, and fails closed below the production workload floor:

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
  --summary target/baseline_raft-vs-rustraft-benchmark/summary.json
```

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
