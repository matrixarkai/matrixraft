# MatrixRaft

[![CI](https://github.com/matrixarkai/matrixraft/actions/workflows/ci.yml/badge.svg)](https://github.com/matrixarkai/matrixraft/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.82-blue.svg)](Cargo.toml)

MatrixRaft is an **open-source Rust Raft consensus library**, licensed under
Apache-2.0 and usable by anyone, in anything. It hosts many Raft groups on a
fixed set of threads rather than a thread per group, and ships the parts a real
deployment needs: a tick driver and worker pool, log storage and a write-ahead
log, snapshots with bounded concurrency, membership changes, lease and
read-index reads, heartbeat merging across groups, and a readiness contract
that reports what the deployment can actually do.

TemporalStore is its largest consumer and drove much of this design, which is
why the crate also carries a compatibility facade and a readiness contract
shaped for that use. Neither is a requirement: the library stands on its own,
and a service with nothing to do with TemporalStore can depend on it.

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
  types mirror the API its consumers already call rather than this crate's, and
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
denominator, not a group being more expensive. Plan accordingly: on this default,
ten thousand groups in one process is ten thousand threads, and the memory is the
smaller part of that bill.

Run it on your own hardware rather than taking these numbers:

```bash
cargo run --release --example group_scaling -- 1024
```

### Or host them on a shared runtime

A thread per group is the default, not the only choice. One line on the context
puts every group the server creates onto a fixed set of threads instead:

```rust
let context = MatrixRaftGroupContextBuilder::new()
    .transport(transport)
    .tick_interval(10)
    .worker_num(8)          // shards, and so the tick capacity
    .shared_runtime(true)
    .build()?;
```

Measured with `examples/idle_tick_cost.rs` on a 16-core Linux box, release build,
one shard per 512 groups, every group a live single voter holding its own
leadership, three repeats:

| groups | threads | cores | ticks delivered | resident | per group |
|---|---|---|---|---|---|
| 1024 | 9 | 0.18–0.25 | ~100% | 25 MiB | 23 KiB |
| 4096 | 17 | 0.77–0.93 | 100.0% | 89 MiB | 21 KiB |
| 16384 | 65 | 3.4–4.2 | ~100% | 342 MiB | 21 KiB |

Sixteen thousand groups on four cores, at every tick the interval asked for, with
`2 * worker_num + 1` threads rather than one per group.

The box was carrying other work throughout -- between two and seven of its
sixteen cores across these runs -- so the cores column is a range and a quiet
machine should read at or below it. The probe takes its own CPU time from
`/proc/self/stat`, so other processes cannot be charged to it, but they do compete
for cache and for scheduling: the 1024-group row read 0.18 to 0.23 at the lighter
load and 0.25 at the heavier.

Every row here was re-run against the current `main` after the changes that
produced it, and reproduced: the same thread counts, the same resident figures to
the megabyte, the same 21 to 23 KiB a group, and every tick the interval asked
for.

What it costs to bring such a store up and take it down, same runs:

| groups | creating | starting | tearing down |
|---|---|---|---|
| 1024 | 0.26s | 0.02s | 0.02s |
| 4096 | 0.56–0.66s | 0.07s | 0.08–0.10s |
| 16384 | 2.4–3.5s | 0.49–0.56s | 0.48–0.56s |

Creating the groups is the larger part, about 0.16ms each: every group opens its
own WAL and writes a first segment, in sequence.

**It is also almost entirely system time, which is worth knowing before trying to
make it faster.** Sampled across a 16,384-group create, the process spent **0.05s
in user code and 4.30s in the kernel** -- 99% of it. So creation is not computation
that more threads would divide; it is syscalls, and the lever is fewer of them per
group rather than more cores. That is the shape the last win had: removing an
fsync of an empty segment took 16,384 groups from 35.0s to 2.33s, and a syscall
trace of a whole run now shows no `fsync` or `fdatasync` at all.

**It is therefore the least portable number on this page.** Being filesystem-bound,
it will move with the storage under it far more than the memory or tick figures
will. Measure it on your own before planning around it.

Four things worth knowing before sizing a host:

- **One file descriptor per group, exactly.** 16,388 held at the peak of a
  16,384-group run — the groups plus the four a process starts with. Raise
  `RLIMIT_NOFILE` above the group count; 16,384 groups under a 10,240 limit fails
  on `PersistentRaftWal::open`, and the error says so.
- **Cores are not proportional to the tick rate, so do not size from one.** This
  bullet used to give `0.2 + 3µs × groups × 1000/tick_interval_ms`, which says a
  tenth of the interval costs very nearly ten times the cores. It does not. The
  same 65,536 groups measured **2.68 cores at a 100ms interval and 14.5 at 10ms**
  -- ten times the ticks for **5.4 times** the cores, not 9.9 -- and both pairs of
  readings were tight (2.700/2.651 and 14.671/14.460). A large part of what a
  hosted store costs is paid for holding the groups rather than for ticking them,
  and the old formula had no term for it: it over-predicts 65,536 groups at 10ms
  by a third and under-predicts the same groups at 100ms by a fifth, in opposite
  directions, which no single constant fixes.

  At a 100ms interval, where the runtime has headroom, cores do scale close to
  linearly with the group count: 0.73, 1.27 and 2.68 for 16,384, 32,768 and
  65,536. Size from a measurement at the interval you will run.

  On a debug build it is about double, so measure the profile you will run.

- **A tick's cost is reaching the group, not the arithmetic it then does.** The
  dozen calls a tick makes into the cluster measure about **60ns together** when
  timed in a loop on one cluster -- but that measures their arithmetic, not what
  they cost a store. Called once each across 2,000 separately allocated clusters the
  same row costs **120-200ns** in allocation order and **200-660ns** visited in a
  scattered one. Over five runs the scattered arm was **2.1 to 9.9 times** the
  hammered one, median about seven; the direction never moved and the size moved by
  a factor of five with how busy the machine was, so take the shape and not the
  multiplier. Cutting the tick body in a running store puts the same region at
  roughly **500ns**, which is the end of that range rather than the beginning.

  So the lever is locality, not algorithmic work in those functions. A tick reaches
  its group through an `Arc`, a `Mutex` and then `BTreeMap`s, and every one of those
  is a pointer chase the prefetcher cannot follow.

  **It is not the visit order.** The ticker holds one bucket per deadline
  millisecond and appends into it, so it hands groups over in the order they were
  registered — which is allocation order, the cheaper of the two arms measured
  above. `examples/tick_working_set.rs` runs the two orders against each other in a
  couple of seconds and prints both the best and the worst of its runs, so a reader
  can see when the box is not resolving the comparison; five runs read 2.3x to 3.8x,
  and the multiplier does not travel.

  What is left is the footprint walked between one visit to a group and the next.
  `RaftCluster` is **520 bytes** of inline state by `size_of`, down from 680 once
  `leader_transfer` and `snapshot_trigger` were boxed — both are `None` in the steady
  state and were 64 and 112 bytes inline. `tests/cluster_footprint.rs` const-asserts
  the figure, so a change in width fails the build rather than a test run. Making a
  group's per-tick state compact is still worth more than making any of these calls
  cleverer; that is the first instalment of it.

  The driver's own share of a tick is no longer part of this. Across runs it reads
  **13 to 18ns** at 1,024, 8,192 **and** 65,536 groups — the flatness is the point
  rather than the figure, and like every level on this page it moves with how busy
  the machine is. It is flat because coming due is an array index into a slot table
  rather than a tree lookup, and because the queue holds one entry per deadline
  millisecond rather than one per group, so a shared interval is one entry however
  many groups are on it. A hosted group doing nothing but tick also
  allocates **nothing**: `tests/tick_window.rs` holds that at 0.05 allocations a
  tick, and `tests/alloc_driver.rs` holds the driver itself at zero.

  `cargo test --release --test tick_call_cost -- --nocapture` prints the row dearest
  first. Read it as a guard against a call becoming *algorithmically* expensive --
  which is what it is for -- and not as what a tick costs.

- **Treat one `cores` reading as indicative, not as a number.** On a 10ms interval
  with a shared box, 16,384 groups read 2.295, 2.700, 2.810, 3.409 and 4.194 cores
  across five runs -- a factor of 1.8 for the same configuration. The readings
  tighten considerably when the runtime is not contending: every 100ms figure above
  repeated to within 10%, and 65,536 at 10ms to within 1.5%, because a saturated
  runtime sits near its ceiling. Repeat a run before believing a difference, and
  especially before believing one between two shard counts: 32 shards against 14 at
  16,384 groups differed by about a fifth, which is less than the spread of either
  arm, so these runs cannot tell them apart.
- **`worker_num` is the shard count, and shards are the tick capacity.** It reads
  like a count of workers; the worker pool is not what limits the tick rate.
- **Watch `SharedRuntimeStats::ticks_skipped`, not `ticks_handed_over`.** A ticker
  that is merely too slow hands nothing over, because a hand-off only happens
  when a group's lock is held — `ticks_handed_over` sat at 0.0% through a 58%
  tick shortfall. `ticks_skipped` counts the ticks the schedule owed and did not
  deliver.

There is also a floor on `tick_interval_ms` that is not the 1ms `validate`
accepts: the ticker advances its clock after a 1ms sleep, and `thread::sleep`
guarantees at least the duration asked for, so a 1ms interval delivers about half
the ticks it asks for. 10ms is held in full at every size above.

#### Past sixteen thousand groups, the tick rate is the constraint

Same probe and box, release, `arm=live`, but **one shard per 1,250 groups rather
than the one per 512 the rows above used**. The shard count is itself a cost, so
the two sets of `cores` figures are not comparable with each other:

| groups | shards | threads | interval | cores | kept up | resident | per group | tearing down |
|---|---|---|---|---|---|---|---|---|
| 16384 | 14 | 29 | 10ms | 2.81 | 100.0% | 340 MiB | 21 KiB | 0.60s |
| 32768 | 27 | 55 | 10ms | 6.79 | 100.0% | 676 MiB | 21 KiB | 1.10s |
| 65536 | 53 | 107 | 10ms | 14.5 | 82-88% | 1348 MiB | 21 KiB | 4.07-50.69s |
| 65536 | 53 | 107 | 100ms | 2.68 | ~100% | 1347 MiB | 21 KiB | 2.13-2.15s |

**The envelope stays linear the whole way: 21 KiB a group and exactly one
descriptor a group at every size.** 65,536 groups is 1.3 GiB resident and 65,540
descriptors, and the resident figure doubles cleanly -- 340, 676, 1348 MiB -- with
no inflection. A group that has been created but not started is **13 KiB** and **no**
descriptor, so starting one costs about **7 KiB** and its one file. An extra
configured voter costs about **1 KiB**: three voters, created and not started, is
15 KiB a group against one voter's 13.

An earlier version of this page said 13 KiB was 15 and 7 was 6. It was comparing a
**three-voter** unstarted group against a **one-voter** started one, because the
probe decided the voter count from whether the row was a live one -- a single flag
for two variables, so the difference between the two rows was starting *and* the
peer count. They are separate arguments now, which is what makes the two figures
above separable at all.

**Every figure on this page is a group with one voter**, which is its own quorum and
so reaches leadership with no transport running. That is what makes an idle hosted
group measurable, and it is not what anyone deploys.

**Configuring three voters instead of one costs about two and a half times the CPU a
tick.** Measured at 8192 groups on a 10ms interval, release, four pairs in ABBA
order: a one-voter group ran 1.03 to 1.23 cores and a three-voter group 2.19 to
3.37, and the four pairwise ratios were 2.62, 2.75, 2.36 and 2.13 -- so **2.1x to
2.8x**, which is tight enough to size from. Both arms held leadership on every
sampled group and delivered every tick the interval asked for, so this is two steady
states and not one of them falling over.

What it is measuring is the per-peer work a tick does: heartbeats, liveness ticking
and catch-up attempts, once for each peer. A one-voter group runs **none** of that,
which is why these pages' figures are a floor rather than an estimate. The two extra
peers here never answer, because the probe starts no transport -- so a healthy
three-node cluster pays this *and* the network on top.

The sizing consequence is the point. 65,536 one-voter groups on a 10ms interval
measured 14.5 cores; the same groups with three voters each would want something
near 36, which is past a sixteen-core machine. A three-voter store at that size
needs a longer interval, fewer groups a machine, or more machines -- and picking one
from the one-voter figure would be picking it from the cheapest shape there is.

**And the cost is linear in the peer count, so it is a formula rather than one
figure.** 8192 groups on a 10ms interval, voter counts run 1, 3, 5, 7 and then 7, 5,
3, 1 so that a drift in load cancels:

| voters | peers | cores | ns a tick | per group |
|---|---|---|---|---|
| 1 | 0 | 0.93 | ~1130 | 20 KiB |
| 3 | 2 | 2.49 | ~3040 | 23 KiB |
| 5 | 4 | 4.07 | ~4970 | 24 KiB |
| 7 | 6 | 5.32 | ~6490 | 24 KiB |

Fitting the ends gives `cores = 0.93 + 0.73 x peers`, which predicts 2.39 at two
peers against 2.49 measured and 3.86 at four against 4.07 -- within 5% without
being fitted to them. So **a tick costs about 1,130ns plus about 890ns for each
peer**, which is the same thing as saying **each peer adds roughly 80% of what the
group's own tick costs**.

Use the ratio rather than the nanoseconds when sizing, because the base moves with
the group count: the same one-voter tick is about 1,130ns at 8192 groups and about
2,200ns at 65,536. Measure the base at your own size and add 80% of it per peer.

**Why the base moves with the group count, and it is not the raft work.** A tick
does the same arithmetic whichever group it reaches. What changes is whether that
group's state is still in cache. `RaftCluster` is **680 bytes** of inline state --
`std::mem::size_of`, so not counting what its maps and vectors point at -- and a
shard ticking thousands of groups walks all of it between one visit to a group and
the next.

`examples/tick_working_set.rs` isolates that, running the same calls the same number
of times in two orders: one group over and over, and a group apiece across two
thousand. Five runs on one Ryzen 5850U read **2.3x to 3.8x** dearer for the second,
the hammered arm at 47 to 65ns and the spread arm at 126 to 248. It takes a couple
of seconds, where seeing the same thing in the table above takes a 65,536-group
sweep.

Two honest limits on that figure. The direction reproduces every time and **the
multiplier does not travel** -- treat it as "a few times", not as 3x. And walking the
group count to find where the rise begins did give a clean step across per-core L2
on a quiet box, but would not reproduce an hour later, with the worst of a row three
to five times its own best; so the example reports two orders rather than a curve,
and this page claims no group count as the boundary.

What it does mean for sizing: fewer groups to a shard keeps more of them warm, so
`worker_num` buys locality as well as tick capacity. The sweep in the table is still
how to pick it -- this only says what the rise is made of.

```bash
cargo run --release --example tick_working_set
```

**Memory does not follow the same shape.** It goes 20, 23, 24, 24 KiB a group across
those four rows -- the first peers cost about 1.5 KiB each and then it flattens,
which is an allocator rounding up rather than a cost per peer. Past about five
voters, a group costs no more to hold than at five. Creating is still
the slow half, about 0.2ms each.

One thing the `create` column does **not** include, in this table or the one above:
the groups' directories. A store needs two a group and the probe lays them all out
before it starts timing, which on a filesystem where `mkdir` is slow is the larger
half of the wall clock -- about **36s** for 16,384 groups against the 3.6s of
creating them, and 3.64s against 0.48s at 2,048. The probe now reports it as its own
`dirs` column so it cannot be mistaken for the `create` figure being wrong, and so
that a sweep taking far longer than its columns explain has somewhere to point.

**What stops being linear is what happens when the tick rate no longer fits the
box.** 65,536 groups on a 10ms interval asks for 6.55 million ticks a second,
which measured 14.5 cores on a sixteen-core machine that was carrying other work.
At that point two things give way:

- `kept up` falls below 100% for the first time, to 82-88%.
- **Tearing the store down stops being bounded.** Four runs of the same 65,536
  groups at 10ms tore down in 4.07s, 6.73s, 13.48s and **50.69s**. The same groups
  at 100ms tore down in 2.13s and 2.15s. A twelvefold spread against a hundredth
  of one is not an algorithm getting slower with size; it is two things starving
  each other.

A server's `Drop` shuts its groups down in batches, and a group leaves the ticker
only once its own shutdown has been answered -- so the teardown competes with the
ticking of every group still waiting its turn. Given tick headroom that costs
nothing measurable: 2.13s, against the 2.99s the same 65,536 groups take to tear
down when they were never started and so never ticked at all. Without headroom the
teardown and the ticking take turns starving, and the time stops being repeatable.

So size the tick **rate**, not only the group count. `groups × 1000 /
tick_interval_ms` is a rate, and it has to fit the cores you have with room left
over for the work the groups are there to do. The same 65,536 groups in the same
1.3 GiB are comfortable on a 100ms interval and marginal on a 10ms one.

Run it on your own hardware:

```bash
cargo run --release --example idle_tick_cost -- 4096 10 3 live 8
```

`live` runs one row in one process, which is the only way the resident figure
divides by the group count and means anything.

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

### And the work goes faster when the pool grows

Holding groups cheaply is worth little if the work does not speed up with
threads. `examples/driver_throughput.rs` sweeps `worker_num` at a fixed group
count. 256 groups, 200k messages, a group receiving a run of 64:

| workers | msgs/sec | |
|---|---|---|
| 1 | 3,054,219 | |
| 2 | 4,719,665 | 1.55x |
| 4 | 4,945,662 | 1.62x |
| 8 | 4,641,568 | 1.52x |

**Throughput is near flat in the group count**, which is the property a store
hosting many groups needs. Measured interleaved across counts, so a drift in
machine load cannot masquerade as a group-count effect — 8 workers, four rounds:

| groups | 8 | 1,024 | 10,000 | 50,000 |
|---|---|---|---|---|
| msgs/sec | 4.8–6.0M | 4.9–5.2M | 4.5–4.9M | 4.4–4.9M |

Fifty thousand groups costs about **13%** against eight — a 6,250-fold increase
in groups for an eighth of the rate.

**Read the access pattern before the number.** The probe takes a `burst`: at
`burst=1` it sends one message per group in turn, so every message lands on a
different worker and wakes a different thread, and throughput *falls* with more
workers (0.06x at eight). That is a wake-up storm, and it is a property of the
sender rather than the pool — a group under load receives a run. Both regimes
are reported because the answers differ:

```bash
cargo run --release --example driver_throughput -- 256 200000 64   # a run per group
cargo run --release --example driver_throughput -- 256 200000 1    # round robin
```

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

Recorded, reported back, and reaching nothing that runs:

<!-- inert-settings: reader_num, executor_num, watched_address_resolver -->

All three are on `MatrixRaftGroupContext`:

- `reader_num`
- `executor_num`
- `watched_address_resolver`

**`MatrixRaftTransportOptions` has none left.** All six were inert while this
section said "three settings on `MatrixRaftGroupContext`" and named none of
them, which left the transport options reading as live. They are listed below
with what each one became.

What the six transport settings became:

| setting | now |
|---|---|
| `cluster_id` | "A cluster id that refuses the other cluster" |
| `dynamic_address_map` | "Addresses that move" |
| `timeout_ms` | "A send that cannot wait for ever" |
| `num_connection_group` | "Connections kept warm per peer" |
| `user_payload_callback_bound` | "What a peer tells a peer" |
| `address_resolver_bound` | never inert — `MatrixRaftTransportBuilder::build` refuses without it |

`timeout_ms` was never in the machine-checked marker even while it was inert,
because the name is also a field on several live types and the guard matches
on a name alone; that is why its entry was a sentence rather than a list item.
`timeout_ms` was never in the marker above even while it was inert, because
it is also a field on several live types and the guard matches on a name
alone; that is why its entry was a sentence rather than a list item. `address_resolver_bound` is not inert either:
`MatrixRaftTransportBuilder::build` refuses without it.

`watched_address_resolver` stays on the list even though
`dynamic_address_map` has left it. They sound like the same setting and are
not: the live one is on `MatrixRaftTransportOptions`, where a transport is
built and can be given a resolver, while this one is on
`MatrixRaftGroupContext`, which builds no transport and so has nothing to
hand a resolver to.

`store_id` belongs to `MatrixRaftNodeCreator` rather than the context. It is
resolved into the wiring report — a group with no creator takes its `peer_id`
— and no further.

Everything else the context carries now reaches something. `worker_num`,
`max_messages_each_poll`, `max_queue_depth`, `driver_batch_bytes` and
`tick_interval_ms` became live when a server gained the option to host its
groups on a shared runtime; they are that runtime's `DriverOptions`.

`tests/settings_contract.rs` checks this list against the source in both
directions, because a list like it is otherwise the first thing to go stale:
a setting gets wired up and the prose still calls it inert, or one is added
and never listed. The second is the worse way round — a host sets a knob and
expects something to happen.

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

### A cluster id that refuses the other cluster

`set_cluster_id` was recorded and read by nothing, which made it the most
misleading knob in the crate: node ids are small integers, so two clusters
that both number their nodes from 1 could append each other's entries
whatever it was set to.

It is now stamped on every request and checked by the receiver **before the
dispatch**, which is the only place it can be checked — a raft node asked to
append entries has already changed its log by the time it could object.

```rust
let options = MatrixRaftTransportBuilder::new()
    .set_cluster_id(42)
    .bind_address_resolver()
    .build()?;

// Both halves, or the id guards nothing: the sender stamps, the server refuses.
let server = options.start_tcp_server("0.0.0.0:7001", handler)?;
let transport = options.into_tcp_transport(peers);
```

`ANY_CLUSTER_ID` is `0` and is the default, so this is opt-in in both
directions: an unconfigured server serves everyone, and an unconfigured
sender is served by everyone. Two *different* non-zero ids do not talk.

Wire-compatible both ways. The id rides in a `TcpRaftTransportEnvelope` whose
`cluster_id` is `serde(default)` and whose request is `flatten`ed, so a peer
that predates the field sends no id and is read as `ANY_CLUSTER_ID`, and one
that predates it ignores the extra field on arrival.

`TcpRaftTransportServer::refused_foreign_cluster` counts the refusals, and is
worth watching rather than merely available: a node given the wrong cluster id
is indistinguishable from a node that is down — every RPC fails — and the
counter is what tells the two apart.

### Addresses that move

A peer that moves from one host to another leaves every send to it failing at
the connect, for ever. Nothing in Raft re-asks where a peer is, and
`set_peer_addr` only helps a host that already knows.

`dynamic_address_map` is what says addresses move. With it set, a connect that
fails re-asks the resolver, applies a **changed** answer, and retries the send
once:

```rust
let options = MatrixRaftTransportBuilder::new()
    .set_dynamic_address_map()
    .bind_address_resolver()
    .build()?;

let transport = options.into_tcp_transport_watching(peers, local_node_id, resolver);
```

Without the flag the same call builds a static map and never consults the
resolver — which is the A/B one of its tests performs, because a flag that
changes nothing observable is how this setting spent its first life.

- **Only on failure.** Re-resolving before every send would put a host's
  service discovery on the Raft hot path, where a slow lookup becomes a slow
  cluster.
- **A repeated address is not a move.** A peer that is merely down resolves to
  where it already was; retrying that pays the connect failure twice and
  counting it would make a down peer look like a moving one.
- **A move drops the pool.** Connections kept open to the old host point
  somewhere that no longer answers.
- `refresh_peer_addr` is the half a host drives itself, for when discovery
  knows before a send has failed. It errors rather than returning `None` when
  there is no resolver, so "already up to date" cannot be read off a transport
  that could never refresh.
- `re_resolved_addresses` counts moves, not lookups.

`peer_addr` now returns `Option<String>` rather than `Option<&str>`: the map
is shared and can change under a resolver, so lending out a borrow into it
would be lending out the answer to a question whose answer moves.

### A replica that appears when a peer asks for one

A request for a group a server does not host is `NodeNotFound`, so a replica
has to exist before the first message can reach it. That makes adding one a
dance across two hosts — create it there, then let the leader find it — when
the leader contacting the host is itself the signal that it should exist.

```rust
let server = MatrixRaftMultiRaftServer::new(context)
    .with_replica_factory(Arc::new(|request: &MatrixRaftReplicaRequest| {
        // Decide WHETHER, not just how. Returning Ok(None) refuses.
        if !placement.should_host(request.group_id) {
            return Ok(None);
        }
        Ok(Some((options_for(request.group_id, request.node_id), 1)))
    }));

let node = server.node_or_create(&request)?;
```

**Opt in, because creating state on an incoming message is a serious thing.**
Without a factory a server behaves exactly as it did, and that is asserted.
With one, a peer can cause allocation here — so pair it with a cluster id
(`start_in_cluster`), or a server that creates replicas on request and serves
every cluster will create them for strangers.

- **Only on absence.** An existing replica is returned without the factory
  being consulted at all, so a host's placement logic stays off the Raft path.
- **Only `node_or_create`.** `node`, `node_mut` and `has_node` still refuse,
  because a read of a replica that does not exist should not bring one into
  being.
- **A refusal looks like a host that never had the group**, which is the only
  honest thing to tell a peer; `on_demand_refusals` is how the *host* tells
  the two apart, since a factory refusing everything leaves a cluster unable
  to add a replica with `NodeNotFound` as the only symptom.
- **A factory answering for a different group is refused** rather than
  registered, or the caller would be handed a node that is not the one it
  asked for.

`MatrixRaftReplicaRequest` carries the group, the node, the asker and the
term — everything that came off the request. There is deliberately **no
host-supplied payload field**: nothing on the wire carries one, and a field
that is always `None` is how a setting becomes a knob that does nothing.
Carrying one would mean threading it through the four `Transport` methods,
which every host implementation of that trait would have to absorb — a
decision worth making on its own.

### A send that cannot wait for ever

A peer that accepts a connection and then never writes — a wedged process, a
host paging, a frozen container — leaves the read with nothing to return and
nothing to fail on. The caller is a Raft send path, so **for ever** means the
leader stops replicating on that thread, not just to the peer that went quiet.
This transport had no socket timeout of any kind.

`timeout_ms` is now what bounds it, and its default is the baseline's
**250ms**:

```rust
let options = MatrixRaftTransportBuilder::new()
    .set_timeout_ms(250)          // the default; shown because it matters
    .bind_address_resolver()
    .build()?;
let transport = options.into_tcp_transport(peers);   // bounded
```

`TcpRaftTransport::new` still waits for ever. The two differ on purpose: a
transport built from options is one a host configured, while `new` is what
this crate's own tests and examples call, and giving every one of those a
250ms deadline would make them fail on a loaded machine rather than on a
broken peer. `with_timeout` is the direct form, and `timeout()` reports which
you have.

- **Reads and writes, not the connect.** A connect to a port nothing is
  listening on fails at once; a connect that hangs is a dropped-packet route,
  and bounding it needs a `SocketAddr` — which means refusing the hostnames
  `TcpStream::connect` accepts. That trade is the host's, so the OS timeout
  stands there and this says so rather than implying a bound it does not give.
- **A timed-out connection is dropped, not pooled.** The peer may answer the
  old request later, and a pooled connection with a stale response waiting on
  it would hand that response to the next caller as the answer to a different
  request.

Its test asserts the send is **bounded**, not that it is fast: ten seconds
against a 250ms timeout. Asserting the 250ms would be a claim about how busy
the machine is.

### Connections kept warm per peer

`num_connection_group` is now `with_connections_per_peer`, which bounds how
many connections to a peer survive their send and spare the next one a
handshake.

**It bounds what is kept, not what is in flight.** A send that finds the pool
empty opens a connection, so concurrent sends are never serialised by this
number — a pool of one still serves three simultaneous appends, and its test
proves that by needing all three in flight for a barrier to release. The
obvious reading of the name is the wrong one, which is why it is stated this
way round.

Its default from options is **1**, the baseline's, while
`TcpRaftTransport::new` keeps `DEFAULT_CONNECTIONS_PER_PEER` (4). So a
transport built from default options keeps fewer warm connections than one
built with `new`, and pays more handshakes when a leader has several appends
in flight to the same follower. A host that cares should say a number. Zero is
raised to one, since a pool that can hold nothing is the opposite of a pool.

### What a peer tells a peer

A replica factory ("A replica that appears when a peer asks for one") gets the
group and node ids off the request and nothing else, so "should this host hold
group 77" has to be answerable from a placement service the **receiver** can
reach. The leader that contacted it already knows.

`user_payload_callback_bound` is now what lets it say so. The sending
transport asks a callback, and the answer rides in the envelope beside the
request:

```rust
let transport = options.into_tcp_transport_with_payload(
    peers,
    Arc::new(|target| Ok(Some(placement.assignment_for(target)?))),
);

let server = TcpRaftTransportServer::start_with_payload_observer(
    "0.0.0.0:7001", handler, cluster_id, Some(observer),
)?;
```

- **Only on a health check** — a (pre-)vote or an append with no entries, which
  `matrixraft_is_health_check_request` decides and a batch inherits if any of
  its requests is one. Those are the frames a peer sends to a peer it may not
  be in contact with yet; an append carrying entries is already the frame
  carrying bulk.
- **Both halves, or nothing happens.** A payload with no observer crosses the
  wire and is dropped; an observer with no sender sees nothing. Neither is an
  error, and that is why they are documented together.
- **Either half may refuse.** A callback that fails fails the send, rather than
  sending a frame that would be understood as "nothing to add". An observer
  that fails refuses the frame, because a host that could not accept what it
  was told should not serve the request as though it had not been told. The
  refusal carries the host's own reason.
- `observed_payloads` counts what an observer accepted; a refusal is counted
  as a refusal, since to the peer the two are the same thing.

**The observer cannot touch the server.** It runs on a connection thread, and
`MatrixRaftMultiRaftServer` is not `Send` — `MatrixRaftNode` holds an
`Rc<RefCell<..>>` scheduler. So a host hands the frame to the thread that owns
its groups, which is what the end-to-end test does over a channel. There is no
way to write it otherwise, and a test that implied there was would not compile.

Wire-compatible both ways, asserted on bytes: an absent payload is not written
at all, and a stamped envelope still decodes as a bare request for a peer that
predates both it and `cluster_id`.

### A cancelled install stays cancelled

`cancel_send` stopped a snapshot going out. Nothing stopped one coming in.

`rollback_install` dropped the partial install, and `install_request` begins a
fresh one whenever it holds none — so **the next chunk to arrive quietly
started the transfer again**. A host removing a replica, or refusing a snapshot
it had decided against, had no way to make the refusal stick.

```rust
downloader.cancel_install();   // stops it, and keeps it stopped
// ... later, when the replica should receive again:
downloader.admit_install();
```

- **A refused chunk is answered, not ignored**, with `throttled: false`. A
  throttle means *later*; this means *not at all*, so it spends the sender's
  retry budget and the send is abandoned — rather than a receiver silently
  discarding a whole snapshot someone is still pushing at it.
- **Refused before the quota is charged.** A chunk this node will not install
  must not count against the bytes it is willing to install, or a cancelled
  replica would exhaust the quota of a node that never took a byte.
- **Nothing reopens it implicitly**, because every implicit route is the
  resurrection this exists to prevent. `begin_send` rebuilds the whole status
  and is the subtlest of them: the two directions share one status struct, and
  starting to *send* says nothing about whether this group will *receive*. It
  carries the flag over, and a test pins that.
- `refused_chunks`, `cancelled_installs` and `install_cancelled` report it.
  Refusals are counted apart from everything else because this is the only
  rejection that means "stop sending" rather than "try again".

**A rollback is not a cancel**, and both are needed. A rollback says "that
attempt was bad, send it again", so the next transfer is legitimately taken; a
cancel says stop. Making rollback behave like a cancel would let one transient
bad chunk stop a replica receiving snapshots at all.

One thing a rollback does *not* do, now asserted rather than assumed: it does
not resume a transfer mid-stream. Handing the installer a chunk at offset 4
with nothing before it is an error, so the sender restarts from the beginning.
That was always true; the test says so now.

### Snapshots that are released

`PersistentRaftSnapshotStore` could save a checkpoint and load one. **Nothing
could delete one.** Every snapshot a group ever wrote stayed on disk, so a
group checkpointing on a timer grew its snapshot directory without bound.

```rust
let report = store.release_checkpoints(applied_index, min_replicated_index)?;
```

A checkpoint is released only when **both** watermarks have passed it:

- `applied_index` — this node has applied past it, so it would never restore
  from it.
- `min_replicated_index` — the **least** index replicated to any peer. A
  follower behind that point may still have to be caught up, and an in-flight
  send is reading that very file; deleting it mid-transfer breaks the transfer
  rather than slowing it.

Taking the **minimum** of the two is what makes this safe to call on a timer:
one lagging follower holds everything, which is the conservative direction. A
host that wants to stop waiting for it should remove it from the group rather
than pass a higher number.

**The newest checkpoint is never released**, whatever the watermarks say,
because a store with nothing in it cannot restore anything. So this can be
called with any arguments — including `u64::MAX` — without leaving the group
unable to recover, and calling it twice is a no-op rather than an emptying.

`list_checkpoints` and `owned_bytes` are how a host sees what it is holding
before deciding to sweep.

**Each checkpoint now has a small descriptor beside it.** Deciding whether to
keep a one-gigabyte snapshot should not mean reading a gigabyte, so the index
and size live in a sidecar — as the comparable engine keeps its snapshot desc
in a separate file from the data. Three details that follow:

- A checkpoint written before sidecars existed still lists, by reading the
  snapshot itself. Slower, which is the right way round: a snapshot with no
  descriptor is merely slow, while a descriptor with no snapshot would
  advertise a checkpoint nothing can load.
- The sidecar is written **after** the checkpoint is synced, for that reason.
- The file's own length wins over the recorded one. The sidecar is a hint, and
  a stale hint would make `owned_bytes` fiction — which is the number a host
  watches to decide whether to sweep at all.

### Storage interfaces the engine does not drive

Three exported traits describe Raft storage. **This crate calls none of them.**

<!-- unwired-traits: Storage, MatrixRaftLogStorage, MatrixRaftGroupStorage -->

- `Storage`
- `MatrixRaftLogStorage`
- `MatrixRaftGroupStorage`

They are faithful and tested — `MatrixRaftMemoryLogStorage` and
`MatrixRaftMemoryGroupStorage` implement them, and
`tests/matrixraft_storage_compat.rs` exercises the segment events, the
compaction report and the two committed indexes. What they are **not** is a
plug-in point: no driver, node or runtime holds one, and a host that
implements one gets a conformant storage engine that this crate will not call.
It has to drive it itself.

The engine's own log is `wal.rs`, which declares **no trait at all**, so log
storage cannot be substituted from outside even in principle. That is the
state of things rather than a recommendation, and it is written down here
because the alternative is someone finding out after writing a backend.

`Storage`'s doc comment used to claim it was "used by production adapters".
It said that while having no implementation, no bound and no caller anywhere
in the crate.

`tests/storage_surface_contract.rs` checks this list against the engine's
sources in both directions, as `tests/settings_contract.rs` does for
settings. If one of these traits is ever wired up the test fails — which is
good news, and a reason to update this section rather than the test.

**One thing that test had to get right**, and is worth repeating anywhere a
scan decides whether something is used: a bound is `: Trait`, never
`::Trait`. The first version of the scan behind it counted both, so every one
of the dozens of `RaftError::Storage(..)` constructions read as a use of the
`Storage` trait and the surface came back thoroughly wired. A scan that cannot
tell a path from a bound will confirm whatever it was pointed at, so the test
asserts its own matcher on both.
### One buffer for every follower

A proposal used to pay **two allocations per follower**, and neither bought
anything.

One was the vector `flush_append_batch` returns. The replication path calls it
once per follower and **discards the result** -- the pipeline's own state is
what matters -- so it was an allocation per follower for a value nobody read.
The other was the lease-acknowledgement list, which started at capacity one
and grew by doubling as peers acknowledged, for a length the node count
already says.

`flush_append_batch_into` writes into a vector the caller owns, so the
replication loop lends one across all the followers, and the acknowledgement
list is sized up front. Measured with `examples/propose_allocations`, per
proposal at a 256-byte payload:

| peers | allocations before | after | bytes before | after |
|---|---|---|---|---|
| 1 | 5.02 | 5.02 | 387 | 387 |
| 3 | 9.06 | 7.06 | 1,402 | 1,194 |
| 5 | 13.10 | 8.10 | 1,968 | 1,328 |
| 7 | 15.14 | 8.14 | 2,406 | 1,398 |
| 9 | 19.18 | 9.18 | 3,101 | 1,597 |

Per additional peer: **1.77 allocations down to 0.35**. At nine peers that is
52% fewer allocations and 48% fewer bytes.

**The one-peer row is the control.** A single-node cluster has no follower to
flush to and one acknowledgement, so it exercises none of this -- and it did
not move, which is what says the saving is the followers and not something
else that changed.

`examples/peer_append_allocations` isolates one follower's share: 2
allocations and 448 bytes before, 1 and 256 after, where the remaining one is
the harness's own entry. The pipeline's per-follower cost is exactly one
`Vec<InflightAppend>` first push -- 192 bytes, four slots of 48 -- and it is
now zero.

**Allocations do not follow the payload**, and did not before this either: the
figure is 8.10 at 64 bytes and 8.10 at 16 KiB. A node's log holds entries
behind an `Arc`, so a proposal's payload is allocated once and shared by every
node rather than copied per node. That part was already right; this is the
handles around it.

`tests/alloc_propose.rs` guards the **slope** rather than the absolute, so a
fixed cost added honestly elsewhere in `propose` does not fail it for an
unrelated reason.

### A stopped cluster refuses

`stop` sets a flag. Of the **121** public operations on `RaftCluster`, exactly
**two** read it -- `propose` and `campaign`. So a host that stopped a group
could still add a peer to it, promote one, resign its leader or trigger a
snapshot, and the operation was carried out on a cluster whose leader lease had
already been invalidated.

The comparable engine gates this at the node: every client-facing call takes a
pass, `Stop` shuts the gate, and a call arriving afterwards is answered "raft
node is not started" rather than served. These now take the same refusal:

| refuses when stopped | |
|---|---|
| `propose` | was already gated |
| `read_index` | |
| `trigger_snapshot` | |
| `add_peer`, `add_learner`, `add_witness` | the last two delegate to the first |
| `remove_peer`, `promote_peer` | |
| `resign_leader` | |

**What is deliberately not gated, and why**, because a list of refusals is only
trustworthy next to one of the exceptions:

- **Inbound message handling** -- `step`, `append_entries_to`, the response
  handlers. This is how a host settles what is already on the wire; refusing it
  would strand those messages rather than finish them.
- **Inspection** -- `status`, `leader_id`, `membership`. A stopped cluster is
  exactly when a host wants to know what it holds.
- **`start`**, or stopping would be one-way.
- **`transfer_leader`**, for a specific reason worth writing down: it reports
  `NoLeader` on a cluster that was never started, and a *running* cluster
  always has a leader because `start` campaigns one. Gating it would make
  `RaftError::NoLeader` unreachable from that entry point and silently replace
  a tested contract with a dead branch. That is a change to make deliberately,
  with the dead path cleaned up -- not a side effect of adding a gate.
- **`compact_logs_through`**, which returns `u64` and has nowhere to put a
  refusal.

`tests/stopped_cluster_contract.rs` pins both halves, including the leader
transfer exception and its reason, so the boundary is a decision rather than
whatever happened to be true.

### One aggregate pass for every heartbeat round

A heartbeat is the highest-frequency thing a leader does -- every tick, to
every peer, whether or not there is anything to replicate -- so a cost here is
multiplied by the tick rate as well as the fan-out.

`broadcast_heartbeat` called the **public** response handler, which recomputes
both group-wide aggregates per response. `refresh_commit_index` collects and
sorts a vector over every quorum node, so a round of N responses allocated **N
vectors of N**. The propose path had already been given the deferral for
exactly this reason, with the inner handler's doc comment explaining that both
aggregates "depend only on the final state, so the intermediate passes cannot
change the outcome". The heartbeat never got it.

Allocations per round, measured with `examples/heartbeat_allocations`:

| peers reached | before | after | per peer before | after |
|---|---|---|---|---|
| 2 | 3.00 | 2.00 | 1.50 | 1.00 |
| 4 | 9.00 | 3.00 | 2.25 | 0.75 |
| 6 | 14.00 | 4.00 | 2.33 | 0.67 |
| 8 | 26.00 | 5.00 | 3.25 | 0.62 |

At eight peers that is **81% fewer allocations** (26.00 to 5.00) and **83%
fewer bytes** (1,888 to 320). The shape matters more than the ratio: the cost
per peer was **rising** with the group and now **falls**, because what is left
is mostly one pass for the round rather than one per peer.

`tests/alloc_heartbeat.rs` asserts that **shape** -- that the per-peer cost
does not grow -- rather than a tuned constant. The defect was the growth, so
that is the property to pin, and it needs no threshold that could drift. A
loose slope bound sits beside it for a regression that stays linear but gets
much worse.

The 40 lease tests in the suite are what check the deferral: moving the
leader-lease quorum evaluation from per-response to once per round is the part
that could have changed behaviour rather than only cost.

**One hypothesis measured and refuted first.** The super-linear shape looked
like the heartbeat's tail probe cloning the log tail per peer, so the probe was
extended to compare a cluster whose followers were behind against one caught
up. The two rows came back **identical**, so the probe was not the cost -- and
the probe-vs-caught-up comparison is still in the example, because a refuted
hypothesis is worth the same as a confirmed one to the next reader.

### A command entry waits for its commit index

A command entry -- one with `is_command` set -- is the shape a host uses for an
operation it must be able to **replay on restart**: a split, a merge, anything
whose effect has to survive a crash. Replay is driven by the committed index.

So applying a command before that index is durable means a crash leaves the
node restarting with a lower committed index and **applying the command a
second time**. For a non-idempotent command that is a corruption no amount of
care in the state machine can see coming.

`is_command` was on every entry and read only to classify entries for
reporting. Nothing held an apply back.

```rust
let eligible = cluster.apply_eligible_through(node_id, durable_commit_index)?;
if let Some(index) = eligible.held_back_at {
    // a command at `index` is waiting for the commit index to be written
}
```

- **It holds back the command and what follows, not the whole apply.** Entries
  before it stay eligible, so a plain entry is never stalled by a command
  behind it.
- **It is a query, not an enforcement.** The host owns its durability and has
  to say what is durable. `mark_apply_task_inflight_with_fence` has it enforced
  for callers who want that; the unchecked `mark_apply_task_inflight` is
  unchanged, because a host whose commands are idempotent -- or which writes
  none -- is paying for nothing.
- **A log with no command entries is never held back**, at any durable index.

The comparable engine reaches the same conclusion the same way: a committed
command entry sets `should_delay_apply` alongside `should_flush_meta`, holding
the apply task until the committed index has been written.

**Where the fence earns its keep.** A `RaftCluster` models every replica in one
process and advances its own applied index, so the existing lower bound already
refuses anything behind it and the fence has no window to narrow there. What it
adds is the **upper** bound, for a host driving its own apply. A test pins that
distinction rather than leaving it to be discovered.

#### A leader no-op is not a command

Found while building the above: a leader's election no-op was written with
`is_command: true`. So the fence held the apply at index 1 of every started
cluster -- and `FsmApplyEntryKind::NoOp` was a variant the actual no-op never
reached, because the classifier maps `is_command` to `Data`.

A no-op carries nothing a host has to replay. It is now `is_command: false`,
which left **all 753 other tests passing** -- the evidence that nothing
depended on the old classification.

### A membership change takes the leader lease with it

A leader lease is held by a **quorum of the current membership**. Change the
membership and the quorum it was measured against is gone:

- a **removed** peer's confirmation would count toward a group it has left,
- an **added** voter raises the bar the lease was measured by,
- a **promoted** learner changes which confirmations are eligible at all,
  because the quorum requires a confirmation's recorded role to match the
  node's current one.

`reset_leader_lease_epoch` handles it the strong way: every confirmation is
cleared and the lease invalidated, so it has to be earned again from the new
membership. That is more conservative than adjusting the member set in place,
which is what the comparable engine's lease does with its own `UpdateMember`.

Two things make this sound rather than merely present. The quorum is counted
over the nodes the cluster holds **now**, not over a member list recorded when
the lease was taken — so a confirmation from a departed node cannot be reached
whatever is left in the map. And a confirmation is only counted while its
epoch, its role and its remaining duration all still match.

**None of it was tested.** Deleting the reset from `add_peer`, `remove_peer` or
`promote_peer` left all 755 tests passing. The code was right and the guarantee
was undefended, so the next person to decide that clearing every confirmation
looks wasteful would have had nothing stop them.
`tests/lease_membership_contract.rs` is what stops them, and each of the three
paths has its own case: the lease drops, and one heartbeat round against the
new membership earns it back — an invalidation, not a one-way door.
### Telling an accepted membership change from a downgraded one

Raft allows only one configuration change to be pending -- in the log and not
yet applied -- at a time. This cluster **downgrades** a second one to an
ordinary entry rather than refusing it: the entry is still appended and
replicated, but it is not registered as a membership change and the membership
does not move.

That is a deliberate choice, pinned by
`membership_proposal_downgrades_second_unapplied_change`. What it cost is that
`propose_with_options` returns only a `LogId`, so a caller got `Ok` either way
-- and a host that proposed a change, saw success and waited for the membership
to move would be waiting for something that was never going to happen.

```rust
match cluster.propose_membership_change(payload)? {
    MembershipProposalOutcome::Accepted { index } => { /* pending at index */ }
    MembershipProposalOutcome::Downgraded { index, pending_at } => {
        // appended at `index` as an ordinary entry; `pending_at` is in the way
    }
}
```

`propose_with_options` is unchanged, and a test pins that too -- the reporting
call exists because the old one cannot say, not to replace it. The shape
follows `transfer_leader_outcome`, which is here for the same reason.

The comparable engine avoids the question by **refusing** the proposal
outright: a change may only be proposed once the leader has applied past the
pending one. That is arguably the better contract, and it is not what this
changes -- reversing a deliberate decision is a different thing from making it
legible, and the first belongs to whoever made it.

**A counter was the other option and the footprint guard refused it.** A
`downgraded_membership_proposals: u64` would have been 8 bytes on every group,
and `tests/cluster_footprint.rs` pins `RaftCluster` at 520 bytes because every
group pays it and a shard walks all of it between visits. The build failed with
exactly that message. The reporting call needs no state at all, so the counter
went and the trade is recorded where the downgrade happens.

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

See what a tick costs when each one reaches a different group:

```bash
cargo run --release --example tick_working_set
```

It prints `size_of::<RaftCluster>()` and the same calls in two orders, one group
hammered against a group apiece across two thousand. Release, because the point is
a memory effect and a debug build buries it. There is more on reading it in
the shared-runtime sizing notes above, under "Why the base moves with the group count".

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

**Contributions are welcome from anyone.** You do not need to ask first and you
do not need to be connected to this project: open an
[issue](https://github.com/matrixarkai/matrixraft/issues) if something is wrong,
unclear or missing, or send a
[pull request](https://github.com/matrixarkai/matrixraft/pulls) if you would
rather fix it yourself. Forks are expected rather than tolerated -- that is what
an Apache-2.0 licence is for.

Please read [`CONTRIBUTING.md`](CONTRIBUTING.md) and the
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
