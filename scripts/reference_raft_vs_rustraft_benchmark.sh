#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: reference_raft_vs_rustraft_benchmark.sh [--rustraft-root PATH] [--reference_raft-root PATH] [--reference_raft-archive PATH] [--reference_raft-bin PATH] [--out PATH] [--summary-out PATH] [--node-count N] [--iterations N] [--batch-size N] [--payload-size-bytes N] [--pass-tolerance-percent PCT] [--release|--debug] [--native-kvbench-adapter] [--build-native-kvbench-adapter]

Runs the standalone RustRaft ReferenceRaft parity benchmark harness from outside
TemporalStore and writes the JSON report to --out plus a compact production
summary to --summary-out.

Environment:
  RUSTRAFT_ROOT   RustRaft checkout. Defaults to this script's parent repo.
  REFERENCE_RAFT_ROOT   ReferenceRaft checkout path. Defaults to RustRaft thirdparty/reference_raft.
  REFERENCE_RAFT_ARCHIVE
                  ReferenceRaft source archive (.zip, .tar, .tar.gz, .tgz). Extracted
                  to a temporary checkout before harness discovery.
  REFERENCE_RAFT_BENCHMARK_BIN  Real ReferenceRaft benchmark harness executable.
  REFERENCE_RAFT_USE_NATIVE_KVBENCH_ADAPTER=1
                  Use RustRaft's native ReferenceRaft kvbench adapter when the full
                  reference_raft_parity_benchmark harness is absent.
  REFERENCE_RAFT_BUILD_NATIVE_KVBENCH_ADAPTER=1
                  Before using the native kvbench adapter, try to build or
                  locate ReferenceRaft example/kv kvserver and kvbench binaries.
  REFERENCE_RAFT_BYTE_ROOT, REFERENCE_RAFT_ROCK_ROOT, REFERENCE_RAFT_BLOCKDB_THIRD_ROOT,
  REFERENCE_RAFT_PROTOC_CMAKE_ROOT
                  Optional read-only dependency source roots. When provided,
                  the native kvbench build uses a temporary symlink overlay
                  instead of modifying the ReferenceRaft checkout's third/ tree.
  BENCHMARK_OUT   Output report path.
  BENCHMARK_SUMMARY_OUT
                  Output compact pass/fail summary path.
  RUSTRAFT_BENCHMARK_ITERATIONS
                  Operations per workload iteration count. Defaults to 128.
  RUSTRAFT_BENCHMARK_BATCH_SIZE
                  Batch size for batched write workloads. Defaults to 16.
  RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES
                  Payload size used by both engines. Defaults to 4096.
  RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT
                  Max allowed RustRaft regression versus ReferenceRaft. Defaults to 10.0.

Production parity is fail-closed: the script requires a real ReferenceRaft harness
and never falls back to the model runner. If the checkout exposes a
reference_raft_parity_benchmark build hook or CMake/Bazel target, the script builds it.
If only ReferenceRaft's native example/kv path is available, the script still writes
report and summary artifacts using the partial native adapter, exits nonzero,
and marks production evidence incomplete.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
reference_raft_root="${REFERENCE_RAFT_ROOT:-$rustraft_root/thirdparty/reference_raft}"
reference_raft_archive="${REFERENCE_RAFT_ARCHIVE:-}"
reference_raft_bin="${REFERENCE_RAFT_BENCHMARK_BIN:-}"
out_path="${BENCHMARK_OUT:-$rustraft_root/target/reference_raft-vs-rustraft-benchmark/report.json}"
summary_path="${BENCHMARK_SUMMARY_OUT:-${out_path%.json}.summary.json}"
cargo_profile=(--release)
build_profile=release
verifier_profile_arg=--release
use_native_kvbench_adapter="${REFERENCE_RAFT_USE_NATIVE_KVBENCH_ADAPTER:-0}"
build_native_kvbench_adapter="${REFERENCE_RAFT_BUILD_NATIVE_KVBENCH_ADAPTER:-0}"
benchmark_iterations="${RUSTRAFT_BENCHMARK_ITERATIONS:-128}"
benchmark_node_count="${RUSTRAFT_BENCHMARK_NODE_COUNT:-5}"
benchmark_batch_size="${RUSTRAFT_BENCHMARK_BATCH_SIZE:-16}"
benchmark_payload_size_bytes="${RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES:-4096}"
benchmark_pass_tolerance_percent="${RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT:-10.0}"
benchmark_max_artifact_age_seconds="${RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS:-86400}"
archive_temp=""
tmp_report=""
native_overlay_temp=""
native_preflight_blockers=""

cleanup() {
  if [[ -n "$tmp_report" ]]; then
    rm -f "$tmp_report"
  fi
  if [[ -n "$archive_temp" ]]; then
    rm -rf "$archive_temp"
  fi
  if [[ -n "$native_overlay_temp" ]]; then
    rm -rf "$native_overlay_temp"
  fi
}
trap cleanup EXIT

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rustraft-root)
      rustraft_root="$2"
      shift 2
      ;;
    --reference_raft-root)
      reference_raft_root="$2"
      shift 2
      ;;
    --reference_raft-archive)
      reference_raft_archive="$2"
      shift 2
      ;;
    --reference_raft-bin)
      reference_raft_bin="$2"
      shift 2
      ;;
    --out)
      out_path="$2"
      shift 2
      ;;
    --summary-out)
      summary_path="$2"
      shift 2
      ;;
    --iterations)
      benchmark_iterations="$2"
      shift 2
      ;;
    --node-count)
      benchmark_node_count="$2"
      shift 2
      ;;
    --batch-size)
      benchmark_batch_size="$2"
      shift 2
      ;;
    --payload-size-bytes)
      benchmark_payload_size_bytes="$2"
      shift 2
      ;;
    --pass-tolerance-percent)
      benchmark_pass_tolerance_percent="$2"
      shift 2
      ;;
    --release)
      cargo_profile=(--release)
      build_profile=release
      verifier_profile_arg=--release
      shift
      ;;
    --debug)
      cargo_profile=()
      build_profile=debug
      verifier_profile_arg=--debug
      shift
      ;;
    --native-kvbench-adapter)
      use_native_kvbench_adapter=1
      shift
      ;;
    --build-native-kvbench-adapter)
      use_native_kvbench_adapter=1
      build_native_kvbench_adapter=1
      shift
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

if [[ ! -f "$rustraft_root/Cargo.toml" ]]; then
  echo "RustRaft root is missing Cargo.toml: $rustraft_root" >&2
  exit 2
fi

require_positive_integer() {
  local blocker_name="$1"
  local value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "benchmark:invalid_${blocker_name}:$value" >&2
    exit 2
  fi
}

require_minimum_integer() {
  local blocker_name="$1"
  local value="$2"
  local minimum="$3"
  if (( value < minimum )); then
    echo "benchmark:${blocker_name}:${value}:${minimum}" >&2
    exit 2
  fi
}

require_maximum_decimal() {
  local blocker_name="$1"
  local value="$2"
  local maximum="$3"
  if [[ ! "$value" =~ ^([0-9]+)(\.[0-9]+)?$ ]]; then
    echo "benchmark:invalid_${blocker_name}:$value" >&2
    exit 2
  fi
  awk -v value="$value" -v maximum="$maximum" 'BEGIN { exit (value <= maximum ? 0 : 1) }' || {
    printf 'benchmark:%s:%.3f:%.3f\n' "$blocker_name" "$value" "$maximum" >&2
    exit 2
  }
}

fsync_file() {
  local path="$1"
  python3 - "$path" <<'PY'
import os
import sys

path = sys.argv[1]
with open(path, "rb") as file:
    os.fsync(file.fileno())
PY
}

fsync_parent_dir() {
  local path="$1"
  python3 - "$path" <<'PY'
import os
import sys

directory = os.path.dirname(os.path.abspath(sys.argv[1])) or "."
flags = os.O_RDONLY
if hasattr(os, "O_DIRECTORY"):
    flags |= os.O_DIRECTORY
fd = os.open(directory, flags)
try:
    os.fsync(fd)
finally:
    os.close(fd)
PY
}

require_positive_integer "iterations_per_workload" "$benchmark_iterations"
require_positive_integer "node_count" "$benchmark_node_count"
require_positive_integer "batch_size" "$benchmark_batch_size"
require_positive_integer "payload_size_bytes" "$benchmark_payload_size_bytes"
require_positive_integer "max_artifact_age_seconds" "$benchmark_max_artifact_age_seconds"
require_minimum_integer "iterations_per_workload_below_production_min" "$benchmark_iterations" 128
require_minimum_integer "node_count_below_production_scale" "$benchmark_node_count" 5
require_minimum_integer "batch_size_below_production_min" "$benchmark_batch_size" 2
require_minimum_integer "payload_size_below_production_min" "$benchmark_payload_size_bytes" 4096
require_maximum_decimal "pass_tolerance_above_production_max" "$benchmark_pass_tolerance_percent" 10.0

if [[ -z "$out_path" ]]; then
  echo "benchmark:invalid_report_path:empty" >&2
  exit 2
fi

if [[ -z "$summary_path" ]]; then
  echo "benchmark:invalid_summary_path:empty" >&2
  exit 2
fi

if [[ "$out_path" == "$summary_path" ]]; then
  echo "benchmark:artifact_path_collision:report_summary:$out_path" >&2
  exit 2
fi

if [[ -d "$out_path" ]]; then
  echo "benchmark:invalid_report_path:is_directory:$out_path" >&2
  exit 2
fi

if [[ -d "$summary_path" ]]; then
  echo "benchmark:invalid_summary_path:is_directory:$summary_path" >&2
  exit 2
fi

extract_reference_raft_archive() {
  if [[ -z "$reference_raft_archive" ]]; then
    return 0
  fi
  if [[ ! -f "$reference_raft_archive" ]]; then
    echo "benchmark:real_reference_raft_missing: ReferenceRaft archive does not exist: $reference_raft_archive" >&2
    exit 2
  fi

  archive_temp="$(mktemp -d)"
  case "$reference_raft_archive" in
    *.zip)
      python3 - "$reference_raft_archive" "$archive_temp" <<'PY'
import sys
import zipfile

with zipfile.ZipFile(sys.argv[1]) as archive:
    archive.extractall(sys.argv[2])
PY
      ;;
    *.tar|*.tar.gz|*.tgz)
      tar -xf "$reference_raft_archive" -C "$archive_temp"
      ;;
    *)
      echo "benchmark:real_reference_raft_missing: unsupported ReferenceRaft archive: $reference_raft_archive" >&2
      exit 2
      ;;
  esac

  local extracted_roots=()
  while IFS= read -r entry; do
    extracted_roots+=("$entry")
  done < <(find "$archive_temp" -mindepth 1 -maxdepth 1 -type d | sort)

  if [[ ${#extracted_roots[@]} -eq 1 ]]; then
    reference_raft_root="${extracted_roots[0]}"
  else
    reference_raft_root="$archive_temp"
  fi
  echo "ReferenceRaft archive extracted: $reference_raft_archive -> $reference_raft_root" >&2
}

find_reference_raft_bin() {
  for candidate in \
    "$reference_raft_root/target/release/reference_raft_parity_benchmark" \
    "$reference_raft_root/target/debug/reference_raft_parity_benchmark" \
    "$reference_raft_root/build/reference_raft_parity_benchmark" \
    "$reference_raft_root/bin/reference_raft_parity_benchmark" \
    "$reference_raft_root/reference_raft_parity_benchmark"; do
    if [[ -x "$candidate" ]]; then
      reference_raft_bin="$candidate"
      return 0
    fi
  done
  return 1
}

try_build_reference_raft_harness() {
  if [[ ! -d "$reference_raft_root" ]]; then
    return 0
  fi

  for build_script in \
    "$reference_raft_root/scripts/build_reference_raft_parity_benchmark.sh" \
    "$reference_raft_root/build_reference_raft_parity_benchmark.sh"; do
    if [[ -f "$build_script" ]]; then
      bash "$build_script" --profile "$build_profile" || return 1
      find_reference_raft_bin && return 0
    fi
  done

  if [[ -f "$reference_raft_root/CMakeLists.txt" ]] && grep -q "reference_raft_parity_benchmark" "$reference_raft_root/CMakeLists.txt"; then
    cmake_build_type=Debug
    if [[ "$build_profile" == "release" ]]; then
      cmake_build_type=Release
    fi
    cmake -S "$reference_raft_root" -B "$reference_raft_root/build" -DCMAKE_BUILD_TYPE="$cmake_build_type"
    cmake --build "$reference_raft_root/build" --target reference_raft_parity_benchmark
    find_reference_raft_bin && return 0
  fi

  if [[ -f "$reference_raft_root/BUILD" ]] && grep -q "reference_raft_parity_benchmark" "$reference_raft_root/BUILD"; then
    (cd "$reference_raft_root" && bazel build //:reference_raft_parity_benchmark)
    mkdir -p "$reference_raft_root/bin"
    if [[ -f "$reference_raft_root/bazel-bin/reference_raft_parity_benchmark" ]]; then
      cp "$reference_raft_root/bazel-bin/reference_raft_parity_benchmark" "$reference_raft_root/bin/reference_raft_parity_benchmark"
    fi
    find_reference_raft_bin && return 0
  fi

  return 0
}

native_reference_raft_capability_report() {
  if [[ ! -d "$reference_raft_root" ]]; then
    echo "ReferenceRaft native capability: root missing ($reference_raft_root)" >&2
    return 0
  fi

  local kvbench=""
  for candidate in \
    "$reference_raft_root/build/example/kv/kvbench" \
    "$reference_raft_root/build/example/kv/kv_benchmark" \
    "$reference_raft_root/bin/kvbench" \
    "$reference_raft_root/kvbench"; do
    if [[ -x "$candidate" ]]; then
      kvbench="$candidate"
      break
    fi
  done

  local source="missing"
  local script="missing"
  local cmake_target="missing"
  [[ -f "$reference_raft_root/example/kv/kv_benchmark.cc" ]] && source="$reference_raft_root/example/kv/kv_benchmark.cc"
  [[ -f "$reference_raft_root/script/bench.sh" ]] && script="$reference_raft_root/script/bench.sh"
  if [[ -f "$reference_raft_root/example/kv/CMakeLists.txt" ]] && grep -q "add_executable(kvbench" "$reference_raft_root/example/kv/CMakeLists.txt"; then
    cmake_target="present"
  fi

  echo "ReferenceRaft native capability: kvbench=${kvbench:-missing} source=$source script=$script cmake_kvbench_target=$cmake_target" >&2
  echo "ReferenceRaft native capability is partial: it can inform single-key client write/read benchmarking, but the full parity harness must still cover batched writes, replication batching, WAL fsync, read-index, lease-read, snapshot install/catch-up, snapshot streaming, and leader transfer under load." >&2
}

find_native_reference_raft_kv_bins() {
  local kvserver_candidate=""
  local kvbench_candidate=""
  local native_cmake_root="${REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT:-$reference_raft_root}"
  for candidate in \
    "$native_cmake_root/build/example/kv/kvserver" \
    "$native_cmake_root/build/example/kv/kv_server" \
    "$reference_raft_root/build/example/kv/kvserver" \
    "$reference_raft_root/build/example/kv/kv_server" \
    "$reference_raft_root/bin/kvserver" \
    "$reference_raft_root/kvserver"; do
    if [[ -x "$candidate" ]]; then
      kvserver_candidate="$candidate"
      break
    fi
  done
  for candidate in \
    "$native_cmake_root/build/example/kv/kvbench" \
    "$native_cmake_root/build/example/kv/kv_benchmark" \
    "$reference_raft_root/build/example/kv/kvbench" \
    "$reference_raft_root/build/example/kv/kv_benchmark" \
    "$reference_raft_root/bin/kvbench" \
    "$reference_raft_root/kvbench"; do
    if [[ -x "$candidate" ]]; then
      kvbench_candidate="$candidate"
      break
    fi
  done

  if [[ -n "$kvserver_candidate" && -n "$kvbench_candidate" ]]; then
    export REFERENCE_RAFT_KVSERVER_BIN="$kvserver_candidate"
    export REFERENCE_RAFT_KVBENCH_BIN="$kvbench_candidate"
    return 0
  fi
  return 1
}

prepare_reference_raft_native_cmake_root() {
  export REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT="$reference_raft_root"
  if [[ -z "${REFERENCE_RAFT_BYTE_ROOT:-}" && -z "${REFERENCE_RAFT_ROCK_ROOT:-}" && -z "${REFERENCE_RAFT_BLOCKDB_THIRD_ROOT:-}" && -z "${REFERENCE_RAFT_PROTOC_CMAKE_ROOT:-}" ]]; then
    return 0
  fi

  native_overlay_temp="$(mktemp -d)"
  shopt -s dotglob nullglob
  for entry in "$reference_raft_root"/*; do
    if [[ "$(basename "$entry")" == "build" ]]; then
      continue
    fi
    ln -s "$entry" "$native_overlay_temp/$(basename "$entry")"
  done
  shopt -u dotglob nullglob
  rm -f "$native_overlay_temp/third"
  mkdir -p "$native_overlay_temp/third"
  if [[ -d "$reference_raft_root/third" ]]; then
    for entry in "$reference_raft_root/third"/*; do
      [[ -e "$entry" ]] || continue
      ln -s "$entry" "$native_overlay_temp/third/$(basename "$entry")"
    done
  fi
  if [[ -n "${REFERENCE_RAFT_BYTE_ROOT:-}" ]]; then
    rm -f "$native_overlay_temp/third/byte"
    ln -s "$REFERENCE_RAFT_BYTE_ROOT" "$native_overlay_temp/third/byte"
  fi
  if [[ -n "${REFERENCE_RAFT_ROCK_ROOT:-}" ]]; then
    rm -f "$native_overlay_temp/third/rock"
    ln -s "$REFERENCE_RAFT_ROCK_ROOT" "$native_overlay_temp/third/rock"
  fi
  if [[ -n "${REFERENCE_RAFT_BLOCKDB_THIRD_ROOT:-}" ]]; then
    rm -f "$native_overlay_temp/third/blockdb-third"
    ln -s "$REFERENCE_RAFT_BLOCKDB_THIRD_ROOT" "$native_overlay_temp/third/blockdb-third"
  fi
  if [[ -n "${REFERENCE_RAFT_PROTOC_CMAKE_ROOT:-}" ]]; then
    rm -f "$native_overlay_temp/third/protobuf"
    ln -s "$REFERENCE_RAFT_PROTOC_CMAKE_ROOT" "$native_overlay_temp/third/protobuf"
  fi
  export REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT="$native_overlay_temp"
  echo "ReferenceRaft native kvbench CMake overlay: source=$REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT byte=${REFERENCE_RAFT_BYTE_ROOT:-checkout} rock=${REFERENCE_RAFT_ROCK_ROOT:-checkout} blockdb=${REFERENCE_RAFT_BLOCKDB_THIRD_ROOT:-checkout} protoc=${REFERENCE_RAFT_PROTOC_CMAKE_ROOT:-checkout}" >&2
}

add_native_preflight_blocker() {
  local blocker="$1"
  if ! grep -Fxq "$blocker" <<<"$native_preflight_blockers"; then
    native_preflight_blockers="${native_preflight_blockers}${blocker}"$'\n'
  fi
  echo "$blocker" >&2
}

reference_raft_native_kvbench_cmake_prereqs_ready() {
  local missing=0
  local native_cmake_root="${REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT:-$reference_raft_root}"
  if [[ -n "${REFERENCE_RAFT_BYTE_ROOT:-}" && ! -f "$REFERENCE_RAFT_BYTE_ROOT/CMakeLists.txt" ]]; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:invalid_reference_raft_byte_root:$REFERENCE_RAFT_BYTE_ROOT"
    missing=1
  fi
  if [[ -n "${REFERENCE_RAFT_BLOCKDB_THIRD_ROOT:-}" && ! -f "$REFERENCE_RAFT_BLOCKDB_THIRD_ROOT/CMakeLists.txt" ]]; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:invalid_reference_raft_blockdb_third_root:$REFERENCE_RAFT_BLOCKDB_THIRD_ROOT"
    missing=1
  fi
  if [[ -n "${REFERENCE_RAFT_PROTOC_CMAKE_ROOT:-}" ]] && \
     ! rg -q "add_executable\\(protoc|add_custom_target\\(protoc|protobuf" "$REFERENCE_RAFT_PROTOC_CMAKE_ROOT" \
       -g CMakeLists.txt -g "*.cmake" 2>/dev/null; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:invalid_reference_raft_protoc_cmake_root:$REFERENCE_RAFT_PROTOC_CMAKE_ROOT"
    missing=1
  fi
  if [[ ! -f "$native_cmake_root/third/byte/CMakeLists.txt" ]]; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:missing_third_byte_cmake"
    missing=1
  fi
  if [[ ! -f "$native_cmake_root/third/blockdb-third/CMakeLists.txt" ]]; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:missing_third_blockdb_cmake"
    missing=1
  fi
  if ! rg -q "add_executable\\(protoc|add_custom_target\\(protoc|protobuf" "$native_cmake_root/third" \
      -g CMakeLists.txt -g "*.cmake" 2>/dev/null; then
    add_native_preflight_blocker "benchmark:reference_raft_native_kvbench_build_blocked:missing_protoc_cmake_target"
    missing=1
  fi
  export REFERENCE_RAFT_NATIVE_PREFLIGHT_BLOCKERS="$native_preflight_blockers"
  return "$missing"
}

try_build_native_kvbench_adapter() {
  if [[ "$build_native_kvbench_adapter" != "1" || ! -d "$reference_raft_root" ]]; then
    return 0
  fi
  if find_native_reference_raft_kv_bins; then
    echo "ReferenceRaft native kvbench binaries found: kvserver=$REFERENCE_RAFT_KVSERVER_BIN kvbench=$REFERENCE_RAFT_KVBENCH_BIN" >&2
    return 0
  fi

  for build_script in \
    "$reference_raft_root/scripts/build_reference_raft_native_kvbench.sh" \
    "$reference_raft_root/build_reference_raft_native_kvbench.sh"; do
    if [[ -f "$build_script" ]]; then
      if bash "$build_script" --profile "$build_profile"; then
        find_native_reference_raft_kv_bins && {
          echo "ReferenceRaft native kvbench binaries built: kvserver=$REFERENCE_RAFT_KVSERVER_BIN kvbench=$REFERENCE_RAFT_KVBENCH_BIN" >&2
          return 0
        }
      else
        echo "benchmark:reference_raft_native_kvbench_build_failed:$build_script" >&2
        return 0
      fi
    fi
  done

  if [[ -f "$reference_raft_root/CMakeLists.txt" && -f "$reference_raft_root/example/kv/CMakeLists.txt" ]] && \
     grep -q "add_executable(kvbench" "$reference_raft_root/example/kv/CMakeLists.txt" && \
     grep -q "add_executable(kvserver" "$reference_raft_root/example/kv/CMakeLists.txt"; then
    prepare_reference_raft_native_cmake_root
    reference_raft_native_kvbench_cmake_prereqs_ready || return 0
    native_cmake_root="${REFERENCE_RAFT_NATIVE_CMAKE_SOURCE_ROOT:-$reference_raft_root}"
    cmake_build_type=Debug
    if [[ "$build_profile" == "release" ]]; then
      cmake_build_type=Release
    fi
    if cmake -S "$native_cmake_root" -B "$native_cmake_root/build" \
        -DCMAKE_BUILD_TYPE="$cmake_build_type" \
        -DREFERENCE_RAFT_WITH_EXAMPLE=ON \
        -DREFERENCE_RAFT_BUILD_TESTS=OFF \
        -DREFERENCE_RAFT_WITH_METRICS=OFF && \
       cmake --build "$native_cmake_root/build" --target kvserver kvbench; then
      find_native_reference_raft_kv_bins && {
        echo "ReferenceRaft native kvbench binaries built: kvserver=$REFERENCE_RAFT_KVSERVER_BIN kvbench=$REFERENCE_RAFT_KVBENCH_BIN" >&2
        return 0
      }
    else
      echo "benchmark:reference_raft_native_kvbench_build_failed:cmake_root_example_kv" >&2
    fi
  fi

  find_native_reference_raft_kv_bins || true
}

extract_reference_raft_archive

if [[ -z "$reference_raft_bin" ]]; then
  find_reference_raft_bin || true
fi

if [[ ! -d "$reference_raft_root" && -z "$reference_raft_bin" ]]; then
  echo "benchmark:real_reference_raft_missing: ReferenceRaft root does not exist: $reference_raft_root" >&2
  exit 2
fi

if [[ -z "$reference_raft_bin" ]]; then
  try_build_reference_raft_harness || {
    echo "benchmark:real_reference_raft_missing: failed to build ReferenceRaft benchmark harness under $reference_raft_root" >&2
    exit 2
  }
fi

if [[ -z "$reference_raft_bin" ]]; then
  adapter="$rustraft_root/scripts/reference_raft_native_kvbench_adapter.sh"
  if [[ -f "$adapter" ]]; then
    try_build_native_kvbench_adapter
    native_reference_raft_capability_report
    if [[ "$use_native_kvbench_adapter" == "1" ]]; then
      echo "ReferenceRaft native kvbench adapter enabled: $adapter" >&2
    else
      echo "ReferenceRaft full benchmark harness missing; using native kvbench adapter for fail-closed partial evidence: $adapter" >&2
    fi
    echo "Production parity is still expected to fail until unsupported workloads are covered by a full ReferenceRaft harness." >&2
    reference_raft_bin="$adapter"
  fi
fi

if [[ -z "$reference_raft_bin" || ! -f "$reference_raft_bin" ]]; then
  native_reference_raft_capability_report
  echo "benchmark:real_reference_raft_missing: no ReferenceRaft benchmark harness found under $reference_raft_root; set REFERENCE_RAFT_BENCHMARK_BIN" >&2
  exit 2
fi
if [[ ! -x "$reference_raft_bin" ]]; then
  echo "benchmark:real_reference_raft_harness_not_executable:$reference_raft_bin" >&2
  exit 2
fi

mkdir -p "$(dirname -- "$out_path")"
mkdir -p "$(dirname -- "$summary_path")"
rm -f -- "$out_path" "$summary_path"

out_dir="$(dirname -- "$out_path")"
out_file="$(basename -- "$out_path")"
tmp_report="$(mktemp "$out_dir/.${out_file}.XXXXXX")"

set +e
REFERENCE_RAFT_ROOT="$reference_raft_root" \
REFERENCE_RAFT_BENCHMARK_BIN="$reference_raft_bin" \
RUSTRAFT_BENCHMARK_PROFILE="$build_profile" \
RUSTRAFT_BENCHMARK_ITERATIONS="$benchmark_iterations" \
RUSTRAFT_BENCHMARK_NODE_COUNT="$benchmark_node_count" \
RUSTRAFT_BENCHMARK_BATCH_SIZE="$benchmark_batch_size" \
RUSTRAFT_BENCHMARK_PAYLOAD_SIZE_BYTES="$benchmark_payload_size_bytes" \
RUSTRAFT_BENCHMARK_PASS_TOLERANCE_PERCENT="$benchmark_pass_tolerance_percent" \
RUSTRAFT_BENCHMARK_SUMMARY_OUT="$summary_path" \
cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example reference_raft_parity_benchmark \
  >"$tmp_report"
benchmark_status=$?
set -e

if [[ -s "$tmp_report" ]]; then
  fsync_file "$tmp_report"
  mv -f -- "$tmp_report" "$out_path"
  fsync_file "$out_path"
  fsync_parent_dir "$out_path"
else
  rm -f "$tmp_report"
fi

if [[ -f "$summary_path" ]]; then
  fsync_file "$summary_path"
  fsync_parent_dir "$summary_path"
fi

echo "ReferenceRaft-vs-RustRaft benchmark report: $out_path"
if [[ -f "$summary_path" ]]; then
  echo "ReferenceRaft-vs-RustRaft benchmark summary: $summary_path"
fi
echo "ReferenceRaft root: $reference_raft_root"
echo "ReferenceRaft benchmark harness: $reference_raft_bin"

if [[ "$benchmark_status" -eq 0 ]]; then
  if [[ ! -f "$out_path" ]]; then
    echo "benchmark:artifact_missing_after_benchmark:report:$out_path" >&2
    exit 1
  fi
  if [[ ! -f "$summary_path" ]]; then
    echo "benchmark:artifact_missing_after_benchmark:summary:$summary_path" >&2
    exit 1
  fi
fi

verifier_status=0
if [[ -f "$out_path" && -f "$summary_path" ]]; then
  set +e
  bash "$rustraft_root/scripts/verify_reference_raft_benchmark_artifacts.sh" \
    --rustraft-root "$rustraft_root" \
    --report "$out_path" \
    --summary "$summary_path" \
    --max-age-seconds "$benchmark_max_artifact_age_seconds" \
    "$verifier_profile_arg"
  verifier_status=$?
  set -e
fi

if [[ "$benchmark_status" -ne 0 ]]; then
  exit "$benchmark_status"
fi
exit "$verifier_status"
