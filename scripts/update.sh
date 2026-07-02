#!/usr/bin/env bash
# scripts/update.sh — one-shot updater for the OCPP gateway stack.
#
# Rebuilds the gateway image, applies Alembic migrations (via the
# migrate-postgres service), and recreates the long-running services
# (`ocpp` + `clickhouse-ingestor`) so the gateway never starts ahead
# of its schema. Works anywhere Docker Engine + Compose v2 runs
# (workstation, VM, bare-metal Linux server).
#
# Scope: this script updates the **gateway only**. The Console (web +
# server) has its own updater at eveys-console/scripts/updater.sh —
# run that separately when you need to bump the Console.
#
# Usage:
#   scripts/update.sh                 # update the gateway
#   scripts/update.sh --no-pull       # don't `git pull` first
#
# Environment overrides:
#   COMPOSE_FILE  Override the compose file path. Defaults to
#                 `deploy/compose/docker-compose.yml`.
#
# Exit codes:
#   0  success
#   1  precondition failed (missing docker, missing compose file)
#   2  one of the build / migrate / start steps failed

set -euo pipefail

# ---------- options ---------------------------------------------------------

DO_PULL=1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-pull) DO_PULL=0; shift ;;
    -h|--help)
      sed -n '2,/^set -euo/p' "$0" | sed -n '2,$p' | sed -n '/^#/p' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

COMPOSE_FILE="${COMPOSE_FILE:-${REPO_ROOT}/deploy/compose/docker-compose.yml}"

# ---------- helpers ---------------------------------------------------------

bold()  { printf '\033[1m%s\033[0m\n' "$*"; }
info()  { printf '==> %s\n' "$*"; }
warn()  { printf '!!  %s\n' "$*" >&2; }
fail()  { printf 'ERR %s\n' "$*" >&2; exit 2; }

need() {
  command -v "$1" >/dev/null 2>&1 || { warn "missing dependency: $1"; exit 1; }
}

# Run `docker compose` against the gateway compose file. Honours .env in
# the repo root the same way the Makefile does.
dc() {
  local env_arg=()
  if [[ -f "${REPO_ROOT}/.env" ]]; then
    env_arg=(--env-file "${REPO_ROOT}/.env")
  fi
  (cd "${REPO_ROOT}" && docker compose -f "${COMPOSE_FILE}" "${env_arg[@]}" "$@")
}

# ---------- preconditions ---------------------------------------------------

need docker
docker compose version >/dev/null 2>&1 || { warn "missing dependency: docker compose"; exit 1; }
[[ -f "${COMPOSE_FILE}" ]] || { warn "compose file not found: ${COMPOSE_FILE}"; exit 1; }

bold "eveys/ocpp gateway one-shot update"
echo "  repo:    ${REPO_ROOT}"
echo "  compose: ${COMPOSE_FILE}"
echo ""

# ---------- gateway stack ---------------------------------------------------

if [[ "${DO_PULL}" -eq 1 ]]; then
  if [[ -d "${REPO_ROOT}/.git" ]]; then
    info "git pull --ff-only in ${REPO_ROOT}"
    if ! (cd "${REPO_ROOT}" && git pull --ff-only); then
      warn "git pull failed — continuing with the working tree as-is"
    fi
  fi
fi

info "building eveys-ocpp image"
(cd "${REPO_ROOT}" && docker build -f deploy/Dockerfile -t eveys-ocpp:dev .) \
  || fail "gateway image build failed"

# Stop the gateway BEFORE the migration runs so the running process
# never sees a half-applied schema. migrate-postgres has
# `restart: on-failure:5` so it won't loop forever if Postgres isn't
# ready yet; failed start here still exits non-zero.
info "stopping eveys-ocpp gateway + clickhouse-ingestor for migration"
dc stop ocpp clickhouse-ingestor >/dev/null

info "running migrate-postgres (alembic upgrade head)"
# --force-recreate so the migrator runs with the NEW image, not a
# cached container from the prior version.
dc up -d --force-recreate migrate-postgres
migrate_status=$(docker wait eveys-ocpp-migrate-postgres 2>/dev/null || echo "wait-failed")
if [[ "${migrate_status}" != "0" ]]; then
  docker logs --tail 100 eveys-ocpp-migrate-postgres || true
  fail "migrate-postgres exited ${migrate_status} — schema NOT applied; gateway left stopped"
fi
info "migrate-postgres OK"

info "running migrate-clickhouse (idempotent)"
dc up -d --force-recreate migrate-clickhouse
ch_status=$(docker wait eveys-ocpp-migrate-clickhouse 2>/dev/null || echo "wait-failed")
if [[ "${ch_status}" != "0" ]]; then
  docker logs --tail 100 eveys-ocpp-migrate-clickhouse || true
  warn "migrate-clickhouse exited ${ch_status} — review logs above"
  # Don't abort: a CH migration failure shouldn't block the gateway
  # from coming back up if the Postgres side is healthy. Operator
  # decides whether to revert.
fi

info "starting eveys-ocpp gateway + clickhouse-ingestor"
dc up -d --force-recreate ocpp clickhouse-ingestor \
  || fail "gateway start failed"

info "waiting for /api/v1/ready"
ready=0
for _ in $(seq 1 30); do
  if docker exec eveys-ocpp python3.13 -c '
import urllib.request, sys
try:
    with urllib.request.urlopen("http://127.0.0.1:8080/api/v1/ready", timeout=2) as r:
        sys.exit(0 if r.status == 200 else 1)
except Exception:
    sys.exit(1)
' >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 2
done
if [[ "${ready}" -eq 1 ]]; then
  info "gateway ready"
else
  warn "gateway did not report /api/v1/ready within 60s — check 'docker logs eveys-ocpp'"
fi

# ---------- done ------------------------------------------------------------

echo ""
bold "done."
echo "  gateway: WS :19000  REST :8080  /api/v1/docs"
