#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 MatrixArkAI
# Fails if a tracked file carries vocabulary that must not appear in this repo.
#
# These names belong to systems and toolchains this project does not ship and
# should not advertise. The check exists because the rest of CI has no opinion
# about prose: a change can add hundreds of occurrences and still pass build,
# clippy, MSRV, licence and coverage, which is exactly what happened once.
set -uo pipefail

files=$(git ls-files)
if [ -z "$files" ]; then
  echo "no tracked files to scan"
  exit 1          # an empty scan must not read as a clean one
fi
echo "scanning $(echo "$files" | wc -l) tracked files"

status=0
report() {
  local label=$1 pattern=$2 exclude=${3:-}
  local hits
  if [ -n "$exclude" ]; then
    hits=$(echo "$files" | xargs -r grep -InE "$pattern" 2>/dev/null | grep -viE "$exclude" || true)
  else
    hits=$(echo "$files" | xargs -r grep -InE "$pattern" 2>/dev/null || true)
  fi
  if [ -n "$hits" ]; then
    echo
    echo "FAIL: $label"
    echo "$hits" | head -40
    local n; n=$(echo "$hits" | wc -l)
    [ "$n" -gt 40 ] && echo "  ... and $((n - 40)) more"
    status=1
  fi
}

# Toolchain and third-party system names.
report "language and toolchain names" \
  'c\+\+|cplusplus|c_plus_plus|cxx|(^|[^A-Za-z])cpp([^A-Za-z]|$)|gtest' 'mcp'
report "third-party product names" \
  '(^|[^A-Za-z])(lark|feishu|wukong|bytedance|byteraft|bytestore|mtcache|openviking|vikingmem)([^A-Za-z]|$)|volcano engine|matrixobjectstore'
# "abase" is a substring of "database", so it only counts on a token boundary.
report "internal datastore name" '(^|[^A-Za-z])abase([^A-Za-z]|$)'
# The C-FFI spelling is CIpsFoo, which a token-start-only pattern misses.
report "retired data-model name" '(^|[^A-Za-z0-9])(C?Ips[A-Z]|ips_|IPS([^a-zA-Z]|$))'

if [ "$status" -ne 0 ]; then
  echo
  echo "These names must not appear in this repository. Use the project's own"
  echo "vocabulary instead; see CONTRIBUTING.md."
  exit 1
fi
echo "OK: no prohibited vocabulary in tracked files"
