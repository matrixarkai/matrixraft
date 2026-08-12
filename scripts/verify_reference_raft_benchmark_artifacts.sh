#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_reference_raft_benchmark_artifacts.sh [--rustraft-root PATH] --report PATH --summary PATH [--max-age-seconds N] [--release|--debug]

Verifies that a saved ReferenceRaft-vs-RustRaft benchmark report and compact summary
match each other, are fresh, and satisfy the production parity gate.
The verifier runs in release mode by default; --debug is for local verifier debugging only.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
report_path=""
summary_path=""
max_age_seconds="${RUSTRAFT_BENCHMARK_MAX_ARTIFACT_AGE_SECONDS:-86400}"
cargo_profile=(--release)

while [[ $# -gt 0 ]]; do
  case "$1" in
    --rustraft-root)
      rustraft_root="$2"
      shift 2
      ;;
    --report)
      report_path="$2"
      shift 2
      ;;
    --summary)
      summary_path="$2"
      shift 2
      ;;
    --max-age-seconds)
      max_age_seconds="$2"
      shift 2
      ;;
    --release)
      cargo_profile=(--release)
      shift
      ;;
    --debug)
      cargo_profile=()
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

if [[ -z "$report_path" || -z "$summary_path" ]]; then
  usage >&2
  exit 2
fi

if [[ "$report_path" == "$summary_path" ]]; then
  echo "benchmark:artifact_path_collision:report_summary:$report_path" >&2
  exit 2
fi

if [[ ! -e "$report_path" ]]; then
  echo "benchmark:artifact_missing:report:$report_path" >&2
  exit 2
fi

if [[ ! -e "$summary_path" ]]; then
  echo "benchmark:artifact_missing:summary:$summary_path" >&2
  exit 2
fi

if [[ ! -f "$report_path" ]]; then
  echo "benchmark:artifact_not_file:report:$report_path" >&2
  exit 2
fi

if [[ ! -f "$summary_path" ]]; then
  echo "benchmark:artifact_not_file:summary:$summary_path" >&2
  exit 2
fi

if [[ ! -r "$report_path" ]]; then
  echo "benchmark:artifact_unreadable:report:$report_path" >&2
  exit 2
fi

if [[ ! -r "$summary_path" ]]; then
  echo "benchmark:artifact_unreadable:summary:$summary_path" >&2
  exit 2
fi

if [[ ! -s "$report_path" ]]; then
  echo "benchmark:artifact_empty:report:$report_path" >&2
  exit 2
fi

if [[ ! -s "$summary_path" ]]; then
  echo "benchmark:artifact_empty:summary:$summary_path" >&2
  exit 2
fi

if [[ ! "$max_age_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "benchmark:invalid_max_artifact_age_seconds:$max_age_seconds" >&2
  exit 2
fi

cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example reference_raft_parity_verify \
  -- \
  --report "$report_path" \
  --summary "$summary_path" \
  --max-age-seconds "$max_age_seconds"
