#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: verify_baseline_raft_benchmark_artifacts.sh [--rustraft-root PATH] --report PATH --summary PATH [--scale PATH] [--readiness-input PATH --readiness-artifact PATH --readiness-labels key=value,... --pressure-snapshot PATH] [--pending-read-index-requests N --pending-bounded-stale-reads N --runtime-timer-pending-ticks N --runtime-timer-max-pending-ticks N --runtime-timer-rejected-ticks N] [--max-age-seconds N] [--release|--debug]

Verifies that a saved BaselineRaft-vs-RustRaft benchmark report and compact
summary match each other, are fresh, and satisfy the production parity gate.
When a scale path is provided, the verifier recomputes the derived QPS and
throughput scale optimization inputs from the report and rejects drift. When
readiness input and readiness artifact paths are provided, the verifier also
recomputes the asserted benchmark/runtime-pressure readiness artifact through
the production-clean benchmark gate and rejects drift in the readiness report,
Prometheus metrics, diagnostic JSON lines, or labels.
Use the optional read-backlog and runtime-timer flags, or matching
RUSTRAFT_BENCHMARK_* environment variables, when verifying an artifact produced
from live release pressure counters instead of the zero/default smoke snapshot.
Memory and latency evidence is read from --pressure-snapshot or matching
RUSTRAFT_BENCHMARK_* environment variables so saved-artifact checks can reuse
the same collector snapshot.
The verifier runs in release mode by default; --debug is for local verifier debugging only.
USAGE
}

script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
rustraft_root="${RUSTRAFT_ROOT:-$(cd -- "$script_dir/.." && pwd)}"
report_path=""
summary_path=""
scale_path=""
readiness_input_path=""
readiness_artifact_path=""
readiness_labels=""
pressure_snapshot_path="${RUSTRAFT_BENCHMARK_PRESSURE_SNAPSHOT:-}"
pending_read_index_requests=""
pending_bounded_stale_reads=""
pending_read_index_warning=""
pending_bounded_stale_read_warning=""
runtime_timer_pending_ticks=""
runtime_timer_max_pending_ticks=""
runtime_timer_accepted_ticks=""
runtime_timer_rejected_ticks=""
runtime_timer_completed_ticks=""
runtime_timer_last_admission_reason=""
runtime_timer_utilization_warning_percent=""
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
    --scale)
      scale_path="$2"
      shift 2
      ;;
    --readiness-input)
      readiness_input_path="$2"
      shift 2
      ;;
    --readiness-artifact)
      readiness_artifact_path="$2"
      shift 2
      ;;
    --readiness-labels)
      readiness_labels="$2"
      shift 2
      ;;
    --pressure-snapshot)
      pressure_snapshot_path="$2"
      shift 2
      ;;
    --pending-read-index-requests)
      pending_read_index_requests="$2"
      shift 2
      ;;
    --pending-bounded-stale-reads)
      pending_bounded_stale_reads="$2"
      shift 2
      ;;
    --pending-read-index-warning)
      pending_read_index_warning="$2"
      shift 2
      ;;
    --pending-bounded-stale-read-warning)
      pending_bounded_stale_read_warning="$2"
      shift 2
      ;;
    --runtime-timer-pending-ticks)
      runtime_timer_pending_ticks="$2"
      shift 2
      ;;
    --runtime-timer-max-pending-ticks)
      runtime_timer_max_pending_ticks="$2"
      shift 2
      ;;
    --runtime-timer-accepted-ticks)
      runtime_timer_accepted_ticks="$2"
      shift 2
      ;;
    --runtime-timer-rejected-ticks)
      runtime_timer_rejected_ticks="$2"
      shift 2
      ;;
    --runtime-timer-completed-ticks)
      runtime_timer_completed_ticks="$2"
      shift 2
      ;;
    --runtime-timer-last-admission-reason)
      runtime_timer_last_admission_reason="$2"
      shift 2
      ;;
    --runtime-timer-utilization-warning-percent)
      runtime_timer_utilization_warning_percent="$2"
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
if [[ -n "$scale_path" ]]; then
  if [[ "$scale_path" == "$report_path" ]]; then
    echo "benchmark:artifact_path_collision:report_scale:$scale_path" >&2
    exit 2
  fi
  if [[ "$scale_path" == "$summary_path" ]]; then
    echo "benchmark:artifact_path_collision:summary_scale:$scale_path" >&2
    exit 2
  fi
fi
if [[ -n "$readiness_input_path" && -z "$readiness_artifact_path" ]]; then
  echo "benchmark:readiness_verification_requires_input_and_artifact" >&2
  exit 2
fi
if [[ -z "$readiness_input_path" && -n "$readiness_artifact_path" ]]; then
  echo "benchmark:readiness_verification_requires_input_and_artifact" >&2
  exit 2
fi
if [[ -n "$readiness_labels" && -z "$readiness_artifact_path" ]]; then
  echo "benchmark:readiness_labels_require_artifact" >&2
  exit 2
fi
if [[ -n "$readiness_input_path" ]]; then
  if [[ "$readiness_input_path" == "$scale_path" ]]; then
    echo "benchmark:artifact_path_collision:scale_readiness_input:$readiness_input_path" >&2
    exit 2
  fi
  if [[ "$readiness_input_path" == "$report_path" ]]; then
    echo "benchmark:artifact_path_collision:report_readiness_input:$readiness_input_path" >&2
    exit 2
  fi
  if [[ "$readiness_input_path" == "$summary_path" ]]; then
    echo "benchmark:artifact_path_collision:summary_readiness_input:$readiness_input_path" >&2
    exit 2
  fi
fi
if [[ -n "$readiness_artifact_path" ]]; then
  if [[ "$readiness_artifact_path" == "$scale_path" ]]; then
    echo "benchmark:artifact_path_collision:scale_readiness:$readiness_artifact_path" >&2
    exit 2
  fi
  if [[ "$readiness_artifact_path" == "$report_path" ]]; then
    echo "benchmark:artifact_path_collision:report_readiness:$readiness_artifact_path" >&2
    exit 2
  fi
  if [[ "$readiness_artifact_path" == "$summary_path" ]]; then
    echo "benchmark:artifact_path_collision:summary_readiness:$readiness_artifact_path" >&2
    exit 2
  fi
  if [[ "$readiness_artifact_path" == "$readiness_input_path" ]]; then
    echo "benchmark:artifact_path_collision:readiness_input_artifact:$readiness_artifact_path" >&2
    exit 2
  fi
fi
if [[ -n "$pressure_snapshot_path" ]]; then
  if [[ "$pressure_snapshot_path" == "$report_path" ]]; then
    echo "benchmark:artifact_path_collision:pressure_snapshot_report:$pressure_snapshot_path" >&2
    exit 2
  fi
  if [[ "$pressure_snapshot_path" == "$summary_path" ]]; then
    echo "benchmark:artifact_path_collision:pressure_snapshot_summary:$pressure_snapshot_path" >&2
    exit 2
  fi
  if [[ "$pressure_snapshot_path" == "$scale_path" ]]; then
    echo "benchmark:artifact_path_collision:pressure_snapshot_scale:$pressure_snapshot_path" >&2
    exit 2
  fi
  if [[ "$pressure_snapshot_path" == "$readiness_input_path" ]]; then
    echo "benchmark:artifact_path_collision:pressure_snapshot_readiness_input:$pressure_snapshot_path" >&2
    exit 2
  fi
  if [[ "$pressure_snapshot_path" == "$readiness_artifact_path" ]]; then
    echo "benchmark:artifact_path_collision:pressure_snapshot_readiness_artifact:$pressure_snapshot_path" >&2
    exit 2
  fi
fi

if [[ ! -e "$report_path" ]]; then
  echo "benchmark:artifact_missing:report:$report_path" >&2
  exit 2
fi

if [[ ! -e "$summary_path" ]]; then
  echo "benchmark:artifact_missing:summary:$summary_path" >&2
  exit 2
fi
if [[ -n "$scale_path" && ! -e "$scale_path" ]]; then
  echo "benchmark:artifact_missing:scale:$scale_path" >&2
  exit 2
fi
if [[ -n "$readiness_input_path" && ! -e "$readiness_input_path" ]]; then
  echo "benchmark:artifact_missing:readiness_input:$readiness_input_path" >&2
  exit 2
fi
if [[ -n "$readiness_artifact_path" && ! -e "$readiness_artifact_path" ]]; then
  echo "benchmark:artifact_missing:readiness:$readiness_artifact_path" >&2
  exit 2
fi
if [[ -n "$pressure_snapshot_path" && ! -e "$pressure_snapshot_path" ]]; then
  echo "benchmark:artifact_missing:pressure_snapshot:$pressure_snapshot_path" >&2
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
if [[ -n "$scale_path" && ! -f "$scale_path" ]]; then
  echo "benchmark:artifact_not_file:scale:$scale_path" >&2
  exit 2
fi
if [[ -n "$readiness_input_path" && ! -f "$readiness_input_path" ]]; then
  echo "benchmark:artifact_not_file:readiness_input:$readiness_input_path" >&2
  exit 2
fi
if [[ -n "$readiness_artifact_path" && ! -f "$readiness_artifact_path" ]]; then
  echo "benchmark:artifact_not_file:readiness:$readiness_artifact_path" >&2
  exit 2
fi
if [[ -n "$pressure_snapshot_path" && ! -f "$pressure_snapshot_path" ]]; then
  echo "benchmark:artifact_not_file:pressure_snapshot:$pressure_snapshot_path" >&2
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
if [[ -n "$scale_path" && ! -r "$scale_path" ]]; then
  echo "benchmark:artifact_unreadable:scale:$scale_path" >&2
  exit 2
fi
if [[ -n "$readiness_input_path" && ! -r "$readiness_input_path" ]]; then
  echo "benchmark:artifact_unreadable:readiness_input:$readiness_input_path" >&2
  exit 2
fi
if [[ -n "$readiness_artifact_path" && ! -r "$readiness_artifact_path" ]]; then
  echo "benchmark:artifact_unreadable:readiness:$readiness_artifact_path" >&2
  exit 2
fi
if [[ -n "$pressure_snapshot_path" && ! -r "$pressure_snapshot_path" ]]; then
  echo "benchmark:artifact_unreadable:pressure_snapshot:$pressure_snapshot_path" >&2
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
if [[ -n "$scale_path" && ! -s "$scale_path" ]]; then
  echo "benchmark:artifact_empty:scale:$scale_path" >&2
  exit 2
fi
if [[ -n "$readiness_input_path" && ! -s "$readiness_input_path" ]]; then
  echo "benchmark:artifact_empty:readiness_input:$readiness_input_path" >&2
  exit 2
fi
if [[ -n "$readiness_artifact_path" && ! -s "$readiness_artifact_path" ]]; then
  echo "benchmark:artifact_empty:readiness:$readiness_artifact_path" >&2
  exit 2
fi
if [[ -n "$pressure_snapshot_path" && ! -s "$pressure_snapshot_path" ]]; then
  echo "benchmark:artifact_empty:pressure_snapshot:$pressure_snapshot_path" >&2
  exit 2
fi
if [[ -n "$pressure_snapshot_path" && -z "$readiness_artifact_path" ]]; then
  echo "benchmark:pressure_snapshot_requires_readiness_artifact" >&2
  exit 2
fi

if [[ ! "$max_age_seconds" =~ ^[1-9][0-9]*$ ]]; then
  echo "benchmark:invalid_max_artifact_age_seconds:$max_age_seconds" >&2
  exit 2
fi

verifier_args=(
  --report "$report_path"
  --summary "$summary_path"
  --max-age-seconds "$max_age_seconds"
)
if [[ -n "$scale_path" ]]; then
  verifier_args+=(--scale "$scale_path")
fi
if [[ -n "$readiness_input_path" ]]; then
  verifier_args+=(--readiness-input "$readiness_input_path")
fi
if [[ -n "$readiness_artifact_path" ]]; then
  verifier_args+=(--readiness-artifact "$readiness_artifact_path")
fi
if [[ -n "$readiness_labels" ]]; then
  verifier_args+=(--readiness-labels "$readiness_labels")
fi
if [[ -n "$pressure_snapshot_path" ]]; then
  verifier_args+=(--pressure-snapshot "$pressure_snapshot_path")
fi
if [[ -n "$pending_read_index_requests" ]]; then
  verifier_args+=(--pending-read-index-requests "$pending_read_index_requests")
fi
if [[ -n "$pending_bounded_stale_reads" ]]; then
  verifier_args+=(--pending-bounded-stale-reads "$pending_bounded_stale_reads")
fi
if [[ -n "$pending_read_index_warning" ]]; then
  verifier_args+=(--pending-read-index-warning "$pending_read_index_warning")
fi
if [[ -n "$pending_bounded_stale_read_warning" ]]; then
  verifier_args+=(--pending-bounded-stale-read-warning "$pending_bounded_stale_read_warning")
fi
if [[ -n "$runtime_timer_pending_ticks" ]]; then
  verifier_args+=(--runtime-timer-pending-ticks "$runtime_timer_pending_ticks")
fi
if [[ -n "$runtime_timer_max_pending_ticks" ]]; then
  verifier_args+=(--runtime-timer-max-pending-ticks "$runtime_timer_max_pending_ticks")
fi
if [[ -n "$runtime_timer_accepted_ticks" ]]; then
  verifier_args+=(--runtime-timer-accepted-ticks "$runtime_timer_accepted_ticks")
fi
if [[ -n "$runtime_timer_rejected_ticks" ]]; then
  verifier_args+=(--runtime-timer-rejected-ticks "$runtime_timer_rejected_ticks")
fi
if [[ -n "$runtime_timer_completed_ticks" ]]; then
  verifier_args+=(--runtime-timer-completed-ticks "$runtime_timer_completed_ticks")
fi
if [[ -n "$runtime_timer_last_admission_reason" ]]; then
  verifier_args+=(--runtime-timer-last-admission-reason "$runtime_timer_last_admission_reason")
fi
if [[ -n "$runtime_timer_utilization_warning_percent" ]]; then
  verifier_args+=(--runtime-timer-utilization-warning-percent "$runtime_timer_utilization_warning_percent")
fi

cargo run \
  --manifest-path "$rustraft_root/Cargo.toml" \
  "${cargo_profile[@]}" \
  --example baseline_raft_parity_verify \
  -- \
  "${verifier_args[@]}"
