#!/usr/bin/env bash
# Thin wrapper around `python -m tools.load`. Exists so the
# spec's acceptance bullet "tools/load/run.sh --quick runs in under
# 2 minutes against `make compose-up`" resolves to a real script.
#
# Usage:
#   tools/load/run.sh --quick           # all scenarios, fast shape
#   tools/load/run.sh --full            # production-shaped, long-running
#   tools/load/run.sh --quick --scenario boot_storm
#
# Environment overrides:
#   LOAD_TARGET     — gateway WS URL (default ws://localhost:19000)
#   LOAD_PROMETHEUS — Prometheus base URL (default http://localhost:9090)
#   LOAD_OUT        — write the report to this path (default stdout)

set -euo pipefail

target="${LOAD_TARGET:-ws://localhost:19000}"
prometheus="${LOAD_PROMETHEUS:-http://localhost:9090}"

args=("--target" "$target" "--prometheus" "$prometheus")
if [[ -n "${LOAD_OUT:-}" ]]; then
    args+=("--out" "$LOAD_OUT")
fi

# Pass through any flags the caller gave us (--quick, --scenario, --json, ...).
exec python -m tools.load "${args[@]}" "$@"
