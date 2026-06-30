#!/usr/bin/env bash
# scripts/update.sh — one-shot updater for the OCPP gateway + Console stacks.
#
# Rebuilds images, applies Alembic migrations (via the migrate-postgres
# service), and recreates the long-running services in the right order
# so the gateway never starts ahead of its schema. Works on a laptop
# (Docker Desktop) and on a VM running plain Docker + Compose.
#
# Usage:
#   scripts/update.sh                 # update both stacks
#   scripts/update.sh --gateway-only  # skip the Console rebuild
#   scripts/update.sh --console-only  # skip the gateway rebuild
#   scripts/update.sh --no-pull       # don't `git pull` either repo
#   scripts/update.sh --console-dir ../eveys-console
#                                     # override the Console repo path
#
# Environment overrides:
#   CONSOLE_DIR   Path to the eveys-console checkout (default: sibling
#                 directory `../eveys-console` next to this repo).
#   COMPOSE_FILE  Override the gateway compose file path. Defaults to
#                 `deploy/compose/docker-compose.yml`.
#   CONSOLE_COMPOSE_FILE
#                 Override the Console compose file path. Defaults to
#                 `<CONSOLE_DIR>/deploy/docker-compose.yml`.
#
# Exit codes:
#   0  success
#   1  precondition failed (missing docker, missing repo, etc.)
#   2  one of the build / migrate / start steps failed

set -euo pipefail

# ---------- options ---------------------------------------------------------

DO_GATEWAY=1
DO_CONSOLE=1
DO_PULL=1
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONSOLE_DIR_DEFAULT="$(cd "${REPO_ROOT}/.." && pwd)/eveys-console"
CONSOLE_DIR="${CONSOLE_DIR:-${CONSOLE_DIR_DEFAULT}}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --gateway-only) DO_CONSOLE=0; shift ;;
    --console-only) DO_GATEWAY=0; shift ;;
    --no-pull)      DO_PULL=0; shift ;;
    --console-dir)  CONSOLE_DIR="${2:?--console-dir needs a path}"; shift 2 ;;
    -h|--help)
      sed -n '2,/^set -euo/p' "$0" | sed -n '2,$p' | sed -n '/^#/p' | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "unknown flag: $1" >&2; exit 1 ;;
  esac
done

COMPOSE_FILE="${COMPOSE_FILE:-${REPO_ROOT}/deploy/compose/docker-compose.yml}"
CONSOLE_COMPOSE_FILE="${CONSOLE_COMPOSE_FILE:-${CONSOLE_DIR}/deploy/docker-compose.yml}"

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
gw_compose() {
  local env_arg=()
  if [[ -f "${REPO_ROOT}/.env" ]]; then
    env_arg=(--env-file "${REPO_ROOT}/.env")
  fi
  (cd "${REPO_ROOT}" && docker compose -f "${COMPOSE_FILE}" "${env_arg[@]}" "$@")
}

# Run `docker compose` against the Console compose file.
co_compose() {
  (cd "${CONSOLE_DIR}" && docker compose -f "${CONSOLE_COMPOSE_FILE}" "$@")
}

pull_repo() {
  local dir="$1"
  [[ "${DO_PULL}" -eq 1 ]] || { info "skipping git pull in ${dir} (--no-pull)"; return; }
  if [[ ! -d "${dir}/.git" ]]; then
    warn "${dir} is not a git checkout — skipping pull"
    return
  fi
  info "git pull --ff-only in ${dir}"
  if ! (cd "${dir}" && git pull --ff-only); then
    warn "git pull failed in ${dir} — continuing with the working tree as-is"
  fi
}

# ---------- preconditions ---------------------------------------------------

need docker
docker compose version >/dev/null 2>&1 || { warn "missing dependency: docker compose"; exit 1; }

if [[ "${DO_GATEWAY}" -eq 1 ]]; then
  [[ -f "${COMPOSE_FILE}" ]] || { warn "gateway compose file not found: ${COMPOSE_FILE}"; exit 1; }
fi
if [[ "${DO_CONSOLE}" -eq 1 ]]; then
  if [[ ! -f "${CONSOLE_COMPOSE_FILE}" ]]; then
    warn "Console compose file not found: ${CONSOLE_COMPOSE_FILE}"
    warn "set CONSOLE_DIR=... or pass --console-dir <path>, or --gateway-only to skip"
    exit 1
  fi
fi

bold "eveys/ocpp + eveys-console one-shot update"
echo "  gateway repo:  ${REPO_ROOT}"
echo "  gateway file:  ${COMPOSE_FILE}"
if [[ "${DO_CONSOLE}" -eq 1 ]]; then
  echo "  console repo:  ${CONSOLE_DIR}"
  echo "  console file:  ${CONSOLE_COMPOSE_FILE}"
fi
echo ""

# ---------- gateway stack ---------------------------------------------------

if [[ "${DO_GATEWAY}" -eq 1 ]]; then
  pull_repo "${REPO_ROOT}"

  info "building eveys-ocpp image"
  (cd "${REPO_ROOT}" && docker build -f deploy/Dockerfile -t eveys-ocpp:dev .) \
    || fail "gateway image build failed"

  # Stop the gateway BEFORE the migration runs so the running process
  # never sees a half-applied schema. migrate-postgres has
  # `restart: on-failure:5` so it won't loop forever if Postgres isn't
  # ready yet; failed start here still exits non-zero.
  info "stopping eveys-ocpp gateway + clickhouse-ingestor for migration"
  gw_compose stop ocpp clickhouse-ingestor >/dev/null

  info "running migrate-postgres (alembic upgrade head)"
  # --force-recreate so the migrator runs with the NEW image, not a
  # cached container from the prior version. Compose waits on
  # service_completed_successfully internally; we re-run explicitly
  # here so the exit code surfaces.
  gw_compose up -d --force-recreate migrate-postgres
  # Block until it exits and capture the exit code.
  migrate_status=$(docker wait eveys-ocpp-migrate-postgres 2>/dev/null || echo "wait-failed")
  if [[ "${migrate_status}" != "0" ]]; then
    docker logs --tail 100 eveys-ocpp-migrate-postgres || true
    fail "migrate-postgres exited ${migrate_status} — schema NOT applied; gateway left stopped"
  fi
  info "migrate-postgres OK"

  info "running migrate-clickhouse (idempotent)"
  gw_compose up -d --force-recreate migrate-clickhouse
  ch_status=$(docker wait eveys-ocpp-migrate-clickhouse 2>/dev/null || echo "wait-failed")
  if [[ "${ch_status}" != "0" ]]; then
    docker logs --tail 100 eveys-ocpp-migrate-clickhouse || true
    warn "migrate-clickhouse exited ${ch_status} — review logs above"
    # Don't abort: a CH migration failure shouldn't block the gateway
    # from coming back up if the Postgres side is healthy. Operator
    # decides whether to revert.
  fi

  info "starting eveys-ocpp gateway + clickhouse-ingestor"
  gw_compose up -d --force-recreate ocpp clickhouse-ingestor \
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
fi

# ---------- console stack ---------------------------------------------------

if [[ "${DO_CONSOLE}" -eq 1 ]]; then
  pull_repo "${CONSOLE_DIR}"

  info "building Console server + web images"
  co_compose build server web \
    || fail "Console image build failed"

  info "recreating Console server + web"
  co_compose up -d --force-recreate server web \
    || fail "Console start failed"
fi

# ---------- done ------------------------------------------------------------

echo ""
bold "done."
[[ "${DO_GATEWAY}" -eq 1 ]] && echo "  gateway: WS :19000  REST :8080  /api/v1/docs"
[[ "${DO_CONSOLE}" -eq 1 ]] && echo "  console: hard-refresh the browser (Cmd-Shift-R) to pick up the new bundle"
