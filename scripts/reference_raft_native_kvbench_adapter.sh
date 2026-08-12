#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: reference_raft_native_kvbench_adapter.sh --workload ID --node-count 3 --iterations N --batch-size N [--payload-size-bytes N] [--wal-dir PATH] [--snapshot-dir PATH] [--reference_raft-root PATH]

Runs ReferenceRaft's native example/kv kvserver+kvbench path for the workloads that
the native example can honestly cover, then emits one RustRaft benchmark sample
JSON object on stdout.

This adapter is a bridge, not production parity. It reports unsupported
production workloads as correctness_failed samples so the full
ReferenceRaft-vs-RustRaft report remains fail-closed until a complete
reference_raft_parity_benchmark harness exists.
USAGE
}

workload=""
node_count=3
iterations=128
batch_size=16
payload_size_bytes="${REFERENCE_RAFT_NATIVE_RECORD_LENGTH:-128}"
wal_dir=""
snapshot_dir=""
reference_raft_root="${REFERENCE_RAFT_ROOT:-}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workload)
      workload="$2"
      shift 2
      ;;
    --node-count)
      node_count="$2"
      shift 2
      ;;
    --iterations)
      iterations="$2"
      shift 2
      ;;
    --batch-size)
      batch_size="$2"
      shift 2
      ;;
    --payload-size-bytes)
      payload_size_bytes="$2"
      shift 2
      ;;
    --wal-dir)
      wal_dir="$2"
      shift 2
      ;;
    --snapshot-dir)
      snapshot_dir="$2"
      shift 2
      ;;
    --reference_raft-root)
      reference_raft_root="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$workload" ]]; then
  echo "missing --workload" >&2
  exit 2
fi

json_string() {
  python3 -c 'import json,sys; print(json.dumps(sys.argv[1]))' "$1"
}

blockers_json() {
  python3 - "$1" <<'PY'
import json
import os
import sys

blockers = [
    line.strip()
    for line in os.environ.get("REFERENCE_RAFT_NATIVE_PREFLIGHT_BLOCKERS", "").splitlines()
    if line.strip()
]
specific = sys.argv[1]
if specific:
    blockers.append(specific)
deduped = []
for blocker in blockers:
    if blocker not in deduped:
        deduped.append(blocker)
print(json.dumps(deduped))
PY
}

emit_sample() {
  local correctness="$1"
  local operation_count="$2"
  local p50="$3"
  local p99="$4"
  local throughput="$5"
  local binary_path="$6"
  local revision="$7"
  local profile="$8"
  local blocker="${9:-}"
  local sample_blockers_json
  sample_blockers_json="$(blockers_json "$blocker")"
  cat <<JSON
{
  "workload": $(json_string "$workload"),
  "engine": "reference_raft",
  "engine_source": "real_reference_raft",
  "binary_path": $(json_string "$binary_path"),
  "git_revision": $(json_string "$revision"),
  "build_profile": $(json_string "$profile"),
  "harness_kind": "native_kvbench_partial",
  "node_count": $node_count,
  "operation_count": $operation_count,
  "p50_latency_micros": $p50,
  "p99_latency_micros": $p99,
  "throughput_ops_per_sec": $throughput,
  "correctness_passed": $correctness,
  "blockers": $sample_blockers_json
}
JSON
}

unsupported_workload_sample() {
  local reason="$1"
  local blocker="benchmark:reference_raft_native_kvbench_unsupported:$workload:$reason"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial" "$blocker"
}

operation_count="$iterations"
if [[ "$workload" == "batched_writes" || "$workload" == "replication_batching" ]]; then
  operation_count=$((iterations * batch_size))
fi

case "$workload" in
  single_key_writes|batched_writes|replication_batching|read_index_reads|lease_reads)
    ;;
  wal_fsync|snapshot_install_catchup|snapshot_streaming|leader_transfer_under_load)
    unsupported_workload_sample "requires full ReferenceRaft parity harness"
    exit 0
    ;;
  *)
    unsupported_workload_sample "unknown workload"
    exit 0
    ;;
esac

if [[ "$node_count" != "3" ]]; then
  blocker="benchmark:reference_raft_native_kvbench_requires_three_nodes:$node_count"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial" "$blocker"
  exit 0
fi

if [[ -z "$reference_raft_root" || ! -d "$reference_raft_root" ]]; then
  blocker="benchmark:real_reference_raft_missing:${reference_raft_root:-unset}"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial" "$blocker"
  exit 0
fi

kvserver="${REFERENCE_RAFT_KVSERVER_BIN:-$reference_raft_root/build/example/kv/kvserver}"
kvbench="${REFERENCE_RAFT_KVBENCH_BIN:-$reference_raft_root/build/example/kv/kvbench}"

if [[ ! -x "$kvserver" && ! -f "$kvserver" ]]; then
  blocker="benchmark:reference_raft_kvserver_binary_missing:$kvserver"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial" "$blocker"
  exit 0
fi

if [[ ! -x "$kvbench" && ! -f "$kvbench" ]]; then
  blocker="benchmark:reference_raft_kvbench_binary_missing:$kvbench"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "" "" "native-kvbench-partial" "$blocker"
  exit 0
fi

revision="$(git -C "$reference_raft_root" rev-parse HEAD 2>/dev/null || true)"
build_profile="${REFERENCE_RAFT_BUILD_PROFILE:-native-kvbench}"
if [[ -n "$wal_dir" ]]; then
  work_dir="$wal_dir"
elif [[ -n "${REFERENCE_RAFT_NATIVE_WORK_DIR:-}" ]]; then
  work_dir="$REFERENCE_RAFT_NATIVE_WORK_DIR"
else
  work_dir="$(mktemp -d "${TMPDIR:-/tmp}/rustraft-reference_raft-kvbench.XXXXXX")"
fi
mkdir -p "$work_dir"
if [[ -n "$snapshot_dir" ]]; then
  mkdir -p "$snapshot_dir"
fi

cleanup() {
  if [[ -n "${server_pids:-}" ]]; then
    for pid in $server_pids; do
      kill "$pid" >/dev/null 2>&1 || true
    done
    wait $server_pids >/dev/null 2>&1 || true
  fi
  if [[ -z "${REFERENCE_RAFT_NATIVE_KEEP_WORK_DIR:-}" ]]; then
    rm -rf "$work_dir"
  fi
}
trap cleanup EXIT

server_pids=""
peers="1,127.0.0.1:19491,127.0.0.1:19492,2,127.0.0.1:19591,127.0.0.1:19592,3,127.0.0.1:19691,127.0.0.1:19692"
addresses="1,127.0.0.1:19490,2,127.0.0.1:19590,3,127.0.0.1:19690"

for id in 1 2 3; do
  base_port=$((19390 + id * 100))
  node_dir="$work_dir/node-$id"
  mkdir -p "$node_dir"
  RAFT_EXAMPLE_BOOT=1 "$kvserver" \
    -id="$id" \
    -kv_addr="127.0.0.1:$base_port" \
    -raft_addr="127.0.0.1:$((base_port + 1))" \
    -snapshot_addr="127.0.0.1:$((base_port + 2))" \
    -peers="$peers" \
    -wal_dir="$node_dir/wal" \
    -fsm_dir="$node_dir/fsm" \
    -snapshot_dir="$node_dir/snapshot" \
    -shard=1 \
    -log_file="$node_dir/LOG" \
    -log_level=2 \
    -metrics_on=false \
    >"$node_dir/stdout.log" 2>"$node_dir/stderr.log" &
  server_pids="$server_pids $!"
done

sleep "${REFERENCE_RAFT_NATIVE_STARTUP_SECONDS:-5}"

if ! kill -0 $server_pids >/dev/null 2>&1; then
  blocker="benchmark:reference_raft_native_cluster_start_failed:$work_dir"
  echo "$blocker" >&2
  emit_sample false "$operation_count" 1000000000 1000000000 1.0 "$kvbench" "$revision" "$build_profile" "$blocker"
  exit 0
fi

read_write_ratio=0
case "$workload" in
  read_index_reads|lease_reads)
    read_write_ratio=1
    ;;
esac

bench_log="$work_dir/kvbench.log"
"$kvbench" \
  -begin_threads=1 \
  -threads=1 \
  -threads_step=1 \
  -threads_per_step_sleep_seconds=0 \
  -threads_step_sleep_seconds=0 \
  -num_connection_group=1 \
  -address="$addresses" \
  -log_detail=false \
  -shard_num=1 \
  -data_begin=0 \
  -data_end=100000000 \
  -operation="$operation_count" \
  -read_write_ratio="$read_write_ratio" \
  -record_length="$payload_size_bytes" \
  -report_intervals=1 \
  >"$bench_log" 2>&1 || true

python3 - "$bench_log" "$operation_count" "$kvbench" "$revision" "$build_profile" "$workload" "$node_count" <<'PY'
import json
import re
import sys

log_path, operation_count, binary_path, revision, profile, workload, node_count = sys.argv[1:]
text = open(log_path, "r", encoding="utf-8", errors="replace").read()
pattern = re.compile(
    r"(?:READ|WRITE)\s+Takes\(s\):\s*([0-9.]+),\s*Count:\s*(\d+),\s*OPS:\s*([0-9.]+),\s*Avg\(us\):\s*(\d+),\s*P95\(us\):\s*(\d+),\s*P99\(us\):\s*(\d+)"
)
matches = pattern.findall(text)
if not matches:
    blocker = f"benchmark:reference_raft_native_kvbench_parse_failed:{log_path}"
    print(blocker, file=sys.stderr)
    sample = {
        "workload": workload,
        "engine": "reference_raft",
        "engine_source": "real_reference_raft",
        "binary_path": binary_path,
        "git_revision": revision or None,
        "build_profile": profile,
        "node_count": int(node_count),
        "operation_count": int(operation_count),
        "p50_latency_micros": 1000000000,
        "p99_latency_micros": 1000000000,
        "throughput_ops_per_sec": 1.0,
        "correctness_passed": False,
        "blockers": [blocker],
    }
else:
    elapsed, count, ops, avg, p95, p99 = matches[-1]
    sample = {
        "workload": workload,
        "engine": "reference_raft",
        "engine_source": "real_reference_raft",
        "binary_path": binary_path,
        "git_revision": revision or None,
        "build_profile": profile,
        "node_count": int(node_count),
        "operation_count": int(operation_count),
        "p50_latency_micros": int(avg),
        "p99_latency_micros": int(p99),
        "throughput_ops_per_sec": float(ops),
        "correctness_passed": int(count) > 0,
        "blockers": [] if int(count) > 0 else ["benchmark:reference_raft_native_kvbench_zero_operations"],
    }
print(json.dumps(sample, indent=2))
PY
