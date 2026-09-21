#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
#
# Runs what CI runs, in one command, so a change can be checked before it is
# pushed rather than after.
#
# Every gate here exits non-zero on failure. A check that prints its result and
# carries on is not a check: this script was written after a local run reported
# a clippy failure, printed it, and pushed the branch anyway.
#
# The environment matters as much as the commands. CI sets RUSTFLAGS and
# RUSTDOCFLAGS to "-D warnings" and passes --locked everywhere, so a run without
# those can pass locally and fail on the branch -- which is how a doc-link error
# reached a pull request.
#
#   scripts/verify_like_ci.sh            # everything
#   scripts/verify_like_ci.sh --quick    # skip the MSRV check and coverage
#
# What it does NOT cover: cargo-deny (advisories move with the world, not with
# your change) and the coverage summary, which is informational in CI.
#
# One limit worth knowing: the vocabulary check scans the index, so a file you
# have not added yet is invisible to it. This script names any such file rather
# than reporting OK over it -- `git add -N .` first if you have new ones.

set -uo pipefail

export CARGO_TERM_COLOR="${CARGO_TERM_COLOR:-always}"
export RUSTFLAGS="${RUSTFLAGS:--D warnings}"
export RUSTDOCFLAGS="${RUSTDOCFLAGS:--D warnings}"

QUICK=0
for arg in "$@"; do
  case "$arg" in
    --quick) QUICK=1 ;;
    -h|--help) sed -n '3,21p' "$0"; exit 0 ;;
    *) echo "unknown argument: $arg" >&2; exit 2 ;;
  esac
done

cd "$(dirname "${BASH_SOURCE[0]}")/.."

FAILED=()
step() {
  local name="$1"; shift
  printf '\n=== %s\n' "$name"
  if "$@"; then
    printf '    ok\n'
  else
    printf '    FAILED\n'
    FAILED+=("$name")
  fi
}

msrv_from_cargo_toml() {
  grep -m1 '^rust-version' Cargo.toml | sed 's/.*"\(.*\)".*/\1/'
}

check_license_headers() {
  local files missing untracked
  # Tracked AND new-but-not-ignored. CI can use `git ls-files` because by the
  # time it runs the file is committed; here the new file is the whole point,
  # and a check that cannot see it passes for the wrong reason.
  files=$(git ls-files --cached --others --exclude-standard '*.rs')
  if [ -z "$files" ]; then
    echo "    no Rust sources found"
    return 0
  fi
  untracked=$(git ls-files --others --exclude-standard '*.rs' | wc -l)
  # shellcheck disable=SC2086
  missing=$(grep -L 'SPDX-License-Identifier: Apache-2.0' $files || true)
  if [ -n "$missing" ]; then
    echo "    missing the SPDX Apache-2.0 header:"
    echo "$missing" | sed 's/^/      /'
    return 1
  fi
  echo "    all $(echo "$files" | wc -l) Rust sources carry the header ($untracked not yet tracked)"
}

check_msrv() {
  local msrv
  msrv=$(msrv_from_cargo_toml)
  if [ -z "$msrv" ]; then
    echo "    no rust-version in Cargo.toml"
    return 1
  fi
  if ! rustup toolchain list 2>/dev/null | grep -q "^$msrv"; then
    echo "    toolchain $msrv is not installed; CI checks it, this run cannot"
    echo "    install it with: rustup toolchain install $msrv"
    return 1
  fi
  cargo "+$msrv" check --all-targets --locked
}

step "format (cargo fmt --all -- --check)" \
  cargo fmt --all -- --check
step "build (cargo build --all-targets --locked)" \
  cargo build --all-targets --locked
step "test (cargo test --locked)" \
  cargo test --locked
step "doc (cargo doc --no-deps --locked, RUSTDOCFLAGS=-D warnings)" \
  cargo doc --no-deps --locked
step "clippy (cargo clippy --all-targets --locked -- -D warnings)" \
  cargo clippy --all-targets --locked -- -D warnings
check_vocabulary() {
  ./scripts/check_prohibited_vocabulary.sh || return 1
  # That script scans `git ls-files`, so a file you have not added yet is
  # invisible to it -- and a new file is exactly what you are about to push.
  local unseen
  unseen=$(git ls-files --others --exclude-standard)
  if [ -n "$unseen" ]; then
    echo "    NOT SCANNED, because they are not in the index yet:"
    echo "$unseen" | sed 's/^/      /'
    echo "    make them visible with:  git add -N ."
  fi
}

step "prohibited vocabulary" check_vocabulary
step "license headers (SPDX)" \
  check_license_headers

if [ "$QUICK" -eq 0 ]; then
  step "MSRV ($(msrv_from_cargo_toml))" check_msrv
else
  printf '\n=== MSRV\n    skipped (--quick)\n'
fi

printf '\n'
if [ "${#FAILED[@]}" -eq 0 ]; then
  printf 'All checks passed.\n'
  exit 0
fi
printf 'FAILED: %s\n' "${FAILED[*]}"
exit 1
