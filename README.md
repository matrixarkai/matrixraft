# MatrixRaft

[![CI](https://github.com/bjmeetsfo/MatrixRaft/actions/workflows/ci.yml/badge.svg)](https://github.com/bjmeetsfo/MatrixRaft/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![MSRV](https://img.shields.io/badge/MSRV-1.82-blue.svg)](Cargo.toml)

MatrixRaft is the TemporalStore-owned Rust Raft readiness and parity contract
library. It is intentionally small: the crate owns the stable public contract
for Raft semantic requirements, readiness evidence, and parity reports, while
TemporalStore owns the storage runtime, data-node integration, and metaserver
integration.

The Cargo crate is named `matrixraft`. Existing exported Rust symbols keep their
`RustRaft*` and `rustraft_*` names for source compatibility with current
TemporalStore consumers.

License: Apache-2.0.

## What This Crate Provides

- `RustRaftSemanticRequirement`
- `RustRaftParityContract`
- `RustRaftParityReport`
- `RustRaftProductionReadinessInput`
- `RustRaftProductionReadinessReport`
- `RustRaftProcessRolloutReadinessReport`
- `RustRaftProductionStatus`
- `RustRaftStorage`
- `RustRaftTransport`
- `InMemoryRaftTransport`
- `RustRaftTransportValidationReport`
- `RustRaftStatusSnapshot`
- `RustRaftMetricNames`
- `RustRaftFaultScenario`
- `rustraft_fault_harness_readiness_report`
- `rustraft_read_safety_decision`
- `rustraft_applied_index_fence_report`
- `rustraft_lease_read_eligibility_report`
- `rustraft_bounded_stale_read_report`
- `rustraft_learner_promotion_decision`
- `rustraft_append_safety_decision`
- `RustRaftReadinessEvidence`
- `RustRaftReadinessSnapshot`
- `rustraft_parity_contract`
- `rustraft_parity_report`
- `rustraft_production_readiness_report`
- `rustraft_data_node_process_rollout_readiness_report`
- `rustraft_meta_process_rollout_readiness_report`
- `rustraft_reference_raft_runtime_capability_report`
- `rustraft_reference_raft_runtime_capability_prometheus`
- `rustraft_public_api_contract`
- `rustraft_open_source_surface`
- `rustraft_temporalstore_adapter_shape`
- `rustraft_temporalstore_extraction_plan`
- `rustraft_metric_names`

The crate is OpenRaft-free and independent of OpenRaft types. TemporalStore
converts its internal readiness evidence into `RustRaftReadinessSnapshot` or
implements `RustRaftReadinessEvidence`, then asks this crate to build a
conservative parity report.

## Production Readiness Status

`rustraft_parity_report` returns both a compatibility boolean and an explicit
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

`rustraft_production_readiness_report` is the fail-closed deployment gate. It
wraps the semantic parity report with runtime evidence for peer pipeline,
snapshot lifecycle, WAL lifecycle, data-node rollout, metaserver rollout,
admin/status observability, fault harness results, and real ReferenceRaft benchmark
parity.
The data-node and metaserver rollout report helpers expose the same fail-closed
process-path checks independently, so TemporalStore and downstream adopters can
validate spawned-process evidence before composing the full production report.
`rustraft_reference_raft_runtime_capability_report` groups the same evidence into
ReferenceRaft-derived runtime capability families: process-path rollout proof,
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
`rustraft_reference_raft_runtime_capability_prometheus` renders that report as generic
`rustraft_reference_raft_*` Prometheus text metrics. Product runtimes such as
TemporalStore can attach their own service labels without duplicating the
capability-matrix logic.

Production readiness also requires real ReferenceRaft benchmark evidence. Model
benchmark runners remain available for unit tests, but
`rustraft_production_readiness_report()` blocks production claims unless
benchmark evidence proves the C++ MatrixRaft reference implementation and the Rust
RustRaft runtime ran the same release-scale workload dimensions and passed
correctness plus the configured latency and throughput threshold. It also
requires `rustraft_fault_harness_readiness_report`
evidence for the ReferenceRaft-derived packet-loss/partition-heal, slow WAL fsync,
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

This standalone version is the Rust equivalent of the C++ TemporalStore +
ReferenceRaft split: RustRaft owns the reusable Raft-facing contracts and model
primitives, while TemporalStore owns only FSM/domain adapters, codecs, process
wiring, and storage-engine integration. RustRaft now owns the stable
node/options, storage, transport, status, metric, safety-policy, WAL record,
snapshot fence, membership, and ReferenceRaft-parity surfaces that TemporalStore can
consume from data-node and metaserver code.

Read safety now includes structured report types for quorum, applied-index
fences, leader lease-read eligibility, and bounded-stale follower reads. These
reports are intended to be filled with observed process-path evidence before a
TemporalStore deployment claims ReferenceRaft-style read-index or lease-read parity.

Transport contracts include fail-fast request/response validators and a generic
in-memory transport router. The router is meant for library tests and harness
adapters; production TemporalStore still owns real process transports and
durable FSM adapters.

The `rustraft_temporalstore_extraction_plan()` API is the typed migration
ledger. It records which Raft responsibilities are already owned by this
standalone crate, which remain pending migration, and which must stay as
TemporalStore-specific adapters.

## Open Source Surface

RustRaft exposes its standalone boundary through public modules for `node`,
`cluster`, `membership`, `wal`, `snapshot`, `transport`, `status`, `metrics`,
`readiness`, `storage`, `benchmark`, and `fault`. The
`rustraft_open_source_surface()` report names those modules, embedding examples,
ReferenceRaft parity matrix entries, benchmark harness APIs, and compatibility
reports so consumers can check the published surface without scraping docs.

RustRaft owns generic Raft contracts, parity/readiness reports, benchmark
interfaces, transport/storage/state-machine traits, and status/metrics surfaces.
TemporalStore keeps adapter docs and implementation details for command codecs,
TemporalEngine apply logic, metaserver scheduling, HTTP/process endpoints, and
storage-object wiring.

`rustraft_standalone_readiness_report()` is the fail-closed status check for a
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
    node: rustraft::node::RaftNodeRuntime<TemporalStoreStateMachine, TemporalTransport>,
    codec: TemporalCommandCodec,
    engine: TemporalEngine,
}
```

`rustraft_temporalstore_adapter_shape()` exposes this as a typed compatibility
report. RustRaft owns consensus behavior inside the node runtime; TemporalStore
owns command encoding, apply semantics, storage engine integration, and
process/admin surfaces.

The fault-harness API names the ReferenceRaft-derived process scenarios that
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

Build a ReferenceRaft-style operational evidence bundle:

```bash
cargo run --example reference_raft_operational_evidence
```

This example validates and prints a
`rustraft.reference_raft_operational_evidence_bundle.v1` JSON document. Real
embedders should replace the example counters with observations from spawned
data-node and metaserver processes, then call
`rustraft_validate_reference_raft_operational_evidence_bundle` before forwarding the
bundle into service readiness or CI gates. The bundle deliberately keeps the
five ReferenceRaft-derived evidence families separate:

- read-index and lease-read safety
- learner, witness, leader-transfer, and joint-consensus membership semantics
- per-peer replication pipeline and reorder-queue pressure
- snapshot sender/downloader, retry, timeout, rollback, and compacted-log rejoin
- WAL segment lifecycle, retained ranges, compaction, and slow-fsync pressure

Run the standalone ReferenceRaft-vs-RustRaft benchmark script from the RustRaft repo:

```bash
REFERENCE_RAFT_ROOT=/path/to/reference_raft \
  bash scripts/reference_raft_vs_rustraft_benchmark.sh \
  --release \
  --node-count 5 \
  --iterations 128 \
  --batch-size 16 \
  --payload-size-bytes 4096 \
  --pass-tolerance-percent 10.0 \
  --out target/reference_raft-vs-rustraft-benchmark/report.json \
  --summary-out target/reference_raft-vs-rustraft-benchmark/summary.json
```

The script does not enter or depend on the TemporalStore checkout. It fails
closed with `benchmark:real_reference_raft_missing` unless `REFERENCE_RAFT_ROOT` contains a
`reference_raft_parity_benchmark` harness or `REFERENCE_RAFT_BENCHMARK_BIN` points to one.
The model runner is intentionally not used for production parity.
Production parity uses release-mode same-workload evidence, requires explicit
`reference_raft_cpp` and `rustraft_rust` implementation identity in the benchmark
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
bash scripts/verify_reference_raft_benchmark_artifacts.sh \
  --release \
  --report target/reference_raft-vs-rustraft-benchmark/report.json \
  --summary target/reference_raft-vs-rustraft-benchmark/summary.json
```

ReferenceRaft's native `example/kv/kv_benchmark.cc` / `kvbench` is detected and
reported as partial evidence, but it does not replace the required JSON parity
harness because it does not cover every production workload.
To generate a normal failing report through that partial path, pass
`--native-kvbench-adapter` after building ReferenceRaft's example `kvserver` and
`kvbench` binaries:

```bash
REFERENCE_RAFT_ROOT=/path/to/reference_raft \
  bash scripts/reference_raft_vs_rustraft_benchmark.sh \
  --native-kvbench-adapter \
  --out target/reference_raft-vs-rustraft-benchmark/native-kvbench-report.json
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
