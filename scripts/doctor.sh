#!/usr/bin/env bash
# scripts/doctor.sh — verify local-dev prerequisites for eveys/ocpp.
#
# Implements task E0-9. Checks every tool listed in
# docs/07-local-dev-setup.md against its minimum version and reports what is
# missing. Exit code 0 = ready to code; exit code 1 = at least one required
# tool is missing or below the minimum version.
#
# Optional tools (kcat, pgcli, etc.) are reported as warnings only.

set -u

# ---- helpers -----------------------------------------------------------------

RED=$(printf '\033[0;31m')
GREEN=$(printf '\033[0;32m')
YELLOW=$(printf '\033[0;33m')
BOLD=$(printf '\033[1m')
RESET=$(printf '\033[0m')

# Disable colors when stdout is not a TTY.
if [ ! -t 1 ]; then
    RED=""; GREEN=""; YELLOW=""; BOLD=""; RESET=""
fi

required_failures=0
optional_warnings=0

ok()    { printf "  %s✓%s %-12s %s\n" "$GREEN" "$RESET" "$1" "$2"; }
miss()  { printf "  %s✗%s %-12s %s\n" "$RED"   "$RESET" "$1" "$2"; required_failures=$((required_failures + 1)); }
warn()  { printf "  %s!%s %-12s %s\n" "$YELLOW" "$RESET" "$1" "$2"; optional_warnings=$((optional_warnings + 1)); }
note()  { printf "  %s%s\n"           "$YELLOW" "$2$RESET"; }

# version_ge "1.2.3" "1.2.0" -> 0 (true) if first ≥ second
version_ge() {
    # Sort lines as version numbers; if the smaller arg sorts first, $1 ≥ $2.
    [ "$(printf '%s\n%s\n' "$2" "$1" | sort -V | head -n1)" = "$2" ]
}

check_required() {
    local name="$1" cmd="$2" min="$3" install_hint="$4" version_extractor="$5"
    if ! command -v "$cmd" >/dev/null 2>&1; then
        miss "$name" "missing — install: $install_hint"
        return
    fi
    local actual
    actual=$(eval "$version_extractor" 2>/dev/null) || true
    if [ -z "$actual" ]; then
        warn "$name" "installed but version could not be parsed (continuing)"
        return
    fi
    if version_ge "$actual" "$min"; then
        ok "$name" "$actual (≥ $min)"
    else
        miss "$name" "$actual (need ≥ $min) — upgrade: $install_hint"
    fi
}

check_optional() {
    local name="$1" cmd="$2" install_hint="$3"
    if command -v "$cmd" >/dev/null 2>&1; then
        ok "$name" "installed"
    else
        warn "$name" "not installed (optional) — install: $install_hint"
    fi
}

# ---- header ------------------------------------------------------------------

printf "%seveys/ocpp — local-dev environment check%s\n" "$BOLD" "$RESET"
printf "Reference: docs/07-local-dev-setup.md\n\n"

# ---- required ----------------------------------------------------------------

printf "%sRequired tools%s\n" "$BOLD" "$RESET"

check_required "Python 3.13" "python3.13" "3.13.0" \
    "brew install python@3.13" \
    "python3.13 --version 2>&1 | awk '{print \$2}'"

check_required "Docker"      "docker"     "24.0.0" \
    "Docker Desktop from docker.com" \
    "docker --version | sed -E 's/.*version ([0-9.]+).*/\\1/'"

# Docker Compose is a docker subcommand in v2; check separately for clarity.
if command -v docker >/dev/null 2>&1; then
    compose_version=$(docker compose version --short 2>/dev/null || echo "")
    if [ -z "$compose_version" ]; then
        miss "Compose" "docker compose v2 not available — update Docker Desktop"
    elif version_ge "$compose_version" "2.0.0"; then
        ok "Compose" "$compose_version (≥ 2.0.0)"
    else
        miss "Compose" "$compose_version (need ≥ 2.0.0)"
    fi
else
    miss "Compose" "depends on docker (not installed)"
fi

check_required "git"         "git"        "2.30.0" \
    "brew install git" \
    "git --version | awk '{print \$3}'"

check_required "uv"          "uv"         "0.5.0" \
    "brew install uv" \
    "uv --version | awk '{print \$2}'"

# ---- k8s path (Path B in 07-local-dev-setup.md) ------------------------------

printf "\n%sKubernetes path (Path B — k3d/kind)%s\n" "$BOLD" "$RESET"
note "" "Required only if you'll test Helm charts, Envoy, or other k8s-specific behavior."

check_required "kubectl"     "kubectl"    "1.30.0" \
    "brew install kubectl" \
    "kubectl version --client -o yaml 2>/dev/null | awk '/gitVersion/ {gsub(/[v\"]/,\"\",\$2); print \$2; exit}'"

check_required "helm"        "helm"       "3.15.0" \
    "brew install helm" \
    "helm version --short 2>/dev/null | sed -E 's/^v([0-9.]+).*/\\1/'"

# Either k3d OR kind is acceptable.
if command -v k3d >/dev/null 2>&1; then
    k3d_version=$(k3d version 2>/dev/null | head -1 | sed -E 's/.*v([0-9.]+).*/\1/')
    ok "k3d"       "$k3d_version (kind not required)"
elif command -v kind >/dev/null 2>&1; then
    kind_version=$(kind version 2>/dev/null | sed -E 's/.*v([0-9.]+).*/\1/')
    ok "kind"      "$kind_version (k3d not required)"
else
    miss "k3d/kind" "neither installed — install one: brew install k3d (or kind)"
fi

# ---- optional ----------------------------------------------------------------

printf "\n%sOptional tools%s\n" "$BOLD" "$RESET"

check_optional "kcat"        "kcat"       "brew install kcat"
check_optional "pgcli"       "pgcli"      "brew install pgcli"
check_optional "redis-cli"   "redis-cli"  "brew install redis"

# clickhouse-client may be a standalone binary (official package) or a
# subcommand of the Homebrew bundled `clickhouse` multi-call binary.
if command -v clickhouse-client >/dev/null 2>&1; then
    ok "clickhouse-client" "installed"
elif command -v clickhouse >/dev/null 2>&1; then
    ok "clickhouse-client" "available as 'clickhouse client' (Homebrew)"
else
    warn "clickhouse-client" "not installed (optional) — install: brew install --cask clickhouse"
fi

# ---- runtime sanity checks ---------------------------------------------------

printf "\n%sRuntime checks%s\n" "$BOLD" "$RESET"

# Docker daemon reachable?
if docker info >/dev/null 2>&1; then
    docker_mem_bytes=$(docker info --format '{{.MemTotal}}' 2>/dev/null || echo 0)
    docker_mem_gb=$(( docker_mem_bytes / 1024 / 1024 / 1024 ))
    if [ "$docker_mem_gb" -ge 6 ]; then
        ok "Docker memory" "${docker_mem_gb} GB (≥ 6 GB recommended)"
    elif [ "$docker_mem_gb" -ge 4 ]; then
        warn "Docker memory" "${docker_mem_gb} GB (recommended: 6 GB for the full stack with ClickHouse)"
    else
        miss "Docker memory" "${docker_mem_gb} GB (need ≥ 4 GB; bump in Docker Desktop → Settings → Resources)"
    fi
else
    miss "Docker daemon" "not running — start Docker Desktop"
fi

# Host-port collisions with the compose stack.
#
# `make compose-up` publishes a fixed set of ports. If something on the
# laptop is already bound to one of them (a Homebrew clickhouse server,
# a stray Postgres, a previous `python -m http.server`), the docker
# binding either fails outright or — on macOS for loopback addresses —
# the existing process wins and our queries silently target the wrong
# server. The latter is what bit us in issue #24, where a Homebrew CH
# on `localhost:8123` swallowed our migrations and the docker CH stayed
# empty.
#
# We probe TCP listen state (not just open sockets) so a `curl` that
# just connected and closed doesn't trigger a false positive.
declare -a expected_ports=(
    "5432:postgres"
    "6379:redis"
    "9092:kafka"
    "8124:clickhouse-http"
    "9001:clickhouse-native"
    "19000:gateway-ws"
    "50051:gateway-grpc"
    "9100:gateway-metrics"
    "8080:gateway-rest"
)
collisions=0
for entry in "${expected_ports[@]}"; do
    port="${entry%%:*}"
    name="${entry##*:}"
    # `lsof -iTCP:<port> -sTCP:LISTEN -nP -F c` lists every listener on
    # the port and returns one `c<full-command-name>` line per process,
    # avoiding the truncation `lsof`'s default tabular output applies to
    # the COMMAND column. We strip docker's own listeners
    # (`com.docker.backend`, `vpnkit`, `Docker`) — those are how compose
    # itself binds.
    holders=$(lsof -iTCP:"$port" -sTCP:LISTEN -nP -F c 2>/dev/null \
        | awk '/^c/ {sub(/^c/,""); print}' \
        | grep -viE 'com\.docker|vpnkit|^Docker$' \
        | sort -u)
    if [ -n "$holders" ]; then
        collisions=$((collisions + 1))
        miss "port $port" "$name port held by non-docker process(es): $(echo "$holders" | tr '\n' ' ')— stop it before \`make compose-up\`"
    fi
done
if [ "$collisions" -eq 0 ]; then
    ok  "ports" "no collisions on the 9 ports compose publishes"
fi

# Free disk space at the repo root.
disk_avail_kb=$(df -k . 2>/dev/null | awk 'NR==2 {print $4}' || echo 0)
disk_avail_gb=$(( disk_avail_kb / 1024 / 1024 ))
if [ "$disk_avail_gb" -ge 20 ]; then
    ok "Disk space"   "${disk_avail_gb} GB free (≥ 20 GB)"
elif [ "$disk_avail_gb" -ge 10 ]; then
    warn "Disk space"  "${disk_avail_gb} GB free (≥ 20 GB recommended for k3d images + Docker volumes)"
else
    miss "Disk space"  "${disk_avail_gb} GB free (need ≥ 10 GB to run the local stack)"
fi

# ---- summary -----------------------------------------------------------------

printf "\n"
if [ "$required_failures" -eq 0 ] && [ "$optional_warnings" -eq 0 ]; then
    printf "%s%sAll checks passed.%s Ready to code.\n" "$GREEN" "$BOLD" "$RESET"
    exit 0
elif [ "$required_failures" -eq 0 ]; then
    printf "%s%sRequired tools OK.%s %d optional warning(s) — see above.\n" \
        "$GREEN" "$BOLD" "$RESET" "$optional_warnings"
    exit 0
else
    printf "%s%s%d required check(s) failed.%s Resolve the items marked %s✗%s and re-run.\n" \
        "$RED" "$BOLD" "$required_failures" "$RESET" "$RED" "$RESET"
    exit 1
fi
