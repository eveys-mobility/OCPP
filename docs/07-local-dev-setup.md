# 07 — Local development setup

> How to bring the full `eveys/ocpp` stack up on an engineer's laptop and verify it before writing code. Covers the data plane (Postgres, Redis, Kafka, ClickHouse) via docker-compose, plus a k3d/kind path for testing Kubernetes-specific behavior (Helm charts, Envoy config, Pod Disruption Budgets).

This document is the **single source of truth for local development**. If a step here doesn't work, fix the doc — don't pass tribal knowledge by Slack.

---

## Audience

- Engineers about to start a Phase 1+ task and need a running stack.
- Anyone reproducing a bug locally.
- New joiners on day 1.

This doc does **not** cover production or staging setup. Production decisions (cloud provider, registry, secrets manager, ingress controller) are open as of 2026-04-29 and will land as ADRs / runbooks later.

---

## Prerequisites

| Tool | Minimum version | Purpose | macOS install |
|---|---|---|---|
| Python | 3.13 | Runtime (per [ADR-0001](./adr/0001-python-asyncio-stack.md)) | `brew install python@3.13` |
| Docker Desktop | 4.30+ | Container runtime | [docker.com](https://www.docker.com/products/docker-desktop/) |
| Docker Compose | v2 (built into Docker Desktop) | Multi-container orchestration | (bundled) |
| `make` | any recent | Task runner | (bundled with Xcode CLT) |
| `git` | 2.30+ | VCS | `brew install git` (Apple's bundled git also works) |
| `uv` | 0.5+ | Python package manager | `brew install uv` |
| `kubectl` | 1.30+ | k8s CLI (for k3d/kind path only) | `brew install kubectl` |
| `helm` | 3.15+ | k8s chart tool (for k3d/kind path only) | `brew install helm` |
| `k3d` *or* `kind` | latest | Local k8s (for k3d/kind path only) | `brew install k3d` or `brew install kind` |

Optional but recommended:

| Tool | Purpose |
|---|---|
| [`kcat`](https://github.com/edenhill/kcat) | Kafka CLI for inspecting topics |
| [`pgcli`](https://www.pgcli.com/) | Better Postgres REPL |
| [`redis-cli`](https://redis.io/docs/latest/develop/connect/cli/) | Redis REPL (bundled with `redis` brew formula) |
| [`clickhouse-client`](https://clickhouse.com/docs/en/interfaces/cli) | ClickHouse REPL (Homebrew installs it as the `clickhouse` multi-call binary; invoke as `clickhouse client`) |

To check everything at once, from the repository root:

```bash
make doctor
```

This runs `scripts/doctor.sh` (delivered as task E0-9), which checks the version of every required tool against the table above and prints exactly what's missing. Exit code 0 = ready to code; exit code 1 = at least one required tool is missing or below the minimum version. Optional tools are reported as warnings.

---

## Two paths

### Path A — docker-compose (default; fastest iteration)

Brings up the full data plane on the host network. Use this for **almost all development**: handler code, gRPC, Kafka producer, persistence. ~30 seconds from `docker compose up` to a healthy stack.

### Path B — k3d / kind (k8s parity)

Brings up a single-node Kubernetes cluster with the same components installed via Helm. Use this when you need to test:

- Helm chart changes
- Envoy config / sticky-hash routing
- Pod Disruption Budgets, rolling restart behavior
- Service-to-Service mTLS
- Resource-limit interactions

Slower (~5 minutes cold start). Not the default.

---

## Path A — docker-compose

### What it brings up

| Service | Container | Host port | Purpose |
|---|---|---|---|
| Postgres 16 | `postgres:16-alpine` | `5432` | Transactional state (`charge_points`, `transactions`) |
| Redis 7 | `redis:7-alpine` | `6379` | Online registry, idempotency cache, pub/sub |
| Kafka (KRaft mode) | `apache/kafka:3.7.0` | `9092` | Event firehose |
| ClickHouse | `clickhouse/clickhouse-server:24` (Ubuntu base; the `:24-alpine` variant is broken on Apple Silicon) | `8123` (HTTP), `9000` (native) | Time-series store ([ADR-0004](./adr/0004-clickhouse-timeseries-store.md)) |
| `eveys/ocpp` | built locally | `19000` (WS, container 9000), `50051` (gRPC), `9100` (metrics) | This service |

> **Note**: ClickHouse native protocol normally listens on `9000`. Our service also wants `9000` inside its container for the WebSocket port. The compose file remaps both onto host ports to avoid the collision: ClickHouse native to `9001`, gateway WS to **`19000`** (container `9000` for both, host published differently). HTTP (`8123`) is unchanged. So a charger or test client connects to `ws://<host>:19000/<cp_id>`.

### Bring it up

From the repo root, once task E0-2 (Python skeleton) and E1-12 (docker-compose for local dev) have landed:

```bash
cd ocpp
make compose-up      # or: docker compose -f deploy/compose/docker-compose.yml up -d
```

Wait for everything to report healthy:

```bash
make compose-status  # or: docker compose ps
```

All five services should be `healthy`. If any is `unhealthy`, see [Troubleshooting](#troubleshooting) below.

### Verify it works

Two scripted smoke tests exercise the stack at different trust levels (see [`10-testing-strategy.md`](./10-testing-strategy.md) and ADR-0024):

```bash
make compose-smoke   # Tier-3 — production-shaped image against the full compose stack
                     # (runs tests/compose_smoke/, owns compose up/down + schema apply)
```

Or the e2e suite, which runs against an already-up compose stack:

```bash
.venv/bin/pytest tests/e2e/ -v       # Tier-2 — uses the running stack
```

Between them, the smoke tests:

1. Connect to Postgres and run a trivial query.
2. Set and read a Redis key.
3. Produce and consume a message from a Kafka topic.
4. Insert and query a row in ClickHouse.
5. Open a WebSocket to the `eveys/ocpp` container, send a `BootNotification`, and assert the response.

If both pass, the stack is good.

### Manual checks

If you want to poke around without the smoke test:

```bash
# Postgres
psql postgres://eveys:eveys@localhost:5432/eveys_ocpp -c "SELECT 1;"

# Redis
redis-cli -p 6379 PING

# Kafka — produce + consume a test message
echo "hello" | kcat -P -b localhost:9092 -t test
kcat -C -b localhost:9092 -t test -c 1

# ClickHouse — HTTP interface
curl 'http://localhost:8123/?query=SELECT%201'

# eveys/ocpp metrics
curl http://localhost:9100/metrics | head -20
```

### Bring it down

```bash
make compose-down              # stop containers, keep volumes
make compose-down-volumes      # stop containers AND wipe data
```

`make compose-down-volumes` is destructive — it deletes Postgres data, Kafka logs, ClickHouse data. Use it when starting fresh after a schema change.

### Reset just one service

To wipe Postgres state without touching Kafka/Redis/ClickHouse:

```bash
docker compose -f deploy/compose/docker-compose.yml rm -fsv postgres
docker volume rm ocpp_postgres_data
docker compose -f deploy/compose/docker-compose.yml up -d postgres
```

---

## Path B — k3d / kind

> Use this only when testing k8s-specific behavior. For day-to-day handler/gRPC work, Path A is faster.

### Bring up the cluster

Using `k3d` (recommended — lighter than `kind`):

```bash
k3d cluster create eveys-ocpp \
    --port "9000:9000@loadbalancer" \
    --port "50051:50051@loadbalancer" \
    --agents 1
```

Or using `kind`:

```bash
kind create cluster --name eveys-ocpp --config deploy/kind/cluster.yaml
```

(`deploy/kind/cluster.yaml` ships with task E5-10 / DR drill work; until then, run with default config.)

Verify:

```bash
kubectl cluster-info
kubectl get nodes
```

### Install the data plane

The data-plane services run as Helm charts in the same cluster:

```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo add clickhouse https://docs.altinity.com/clickhouse-operator/
helm repo update

# Postgres
helm install postgres bitnami/postgresql \
    --set auth.username=eveys \
    --set auth.password=eveys \
    --set auth.database=eveys_ocpp

# Redis (single-node, no auth — dev only)
helm install redis bitnami/redis \
    --set architecture=standalone \
    --set auth.enabled=false

# Kafka (KRaft mode, single broker — dev only)
helm install kafka bitnami/kafka \
    --set kraft.enabled=true \
    --set replicaCount=1

# ClickHouse via Altinity operator (production-like)
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-bundle.yaml
# Skipped on Path B until Phase 4 lands `deploy/k8s/clickhouse-dev.yaml` —
# see the note immediately below.
# kubectl apply -f deploy/k8s/clickhouse-dev.yaml
```

(`deploy/k8s/clickhouse-dev.yaml` is **not** part of E2-13/E2-14 — that work was scoped to the compose stack and the in-cluster ingestion sidecar is per-pod independent. The K8s ClickHouse manifest lands in Phase 4 alongside the load test, when the single-node-vs-`ReplicatedMergeTree` decision is informed by real numbers; see ADR-0020 § "Project conventions implied". Until then, Path B operators get the operator installed but no ClickHouse instance — bring one up with `kubectl run` or use Path A's docker-compose stack for a local ClickHouse.)

Wait for everything to be `Ready`:

```bash
kubectl get pods --watch
```

### Install `eveys/ocpp` via its own Helm chart

```bash
make build-image                                   # build local image
k3d image import eveys-ocpp:dev -c eveys-ocpp      # k3d-specific
# or for kind:
# kind load docker-image eveys-ocpp:dev --name eveys-ocpp

helm install ocpp deploy/helm/eveys-ocpp \
    --set image.tag=dev \
    --set image.pullPolicy=Never
```

### Verify

```bash
kubectl get pods -l app=eveys-ocpp
kubectl logs -l app=eveys-ocpp -f
# Smoke against the cluster: port-forward the WS service and run the e2e suite.
kubectl port-forward svc/eveys-ocpp 9000:9000 50051:50051 &
.venv/bin/pytest tests/e2e/ -v
```

### Tear down

```bash
k3d cluster delete eveys-ocpp
# or
kind delete cluster --name eveys-ocpp
```

---

## Service credentials (local-dev only)

These are **hardcoded for local development**. Real credentials are managed by the secrets manager (TBD; see ADR backlog) and never appear in repo files.

| Service | Username | Password | Database / Notes |
|---|---|---|---|
| Postgres | `eveys` | `eveys` | DB: `eveys_ocpp` |
| Redis | (none) | (none) | DB index `0` |
| Kafka | (none) | (none) | PLAINTEXT, no SASL |
| ClickHouse | `default` | (none) | DB: `eveys_ocpp` |

> **Never copy these credentials into staging or production.** They exist for laptop development only. The compose file marks them with a comment to prevent confusion.

---

## Troubleshooting

### `docker compose up` hangs on Kafka

Kafka in KRaft mode generates a cluster ID on first start. If a stale ID is present from a previous run, startup hangs. Fix:

```bash
make compose-down-volumes
make compose-up
```

### Postgres rejects connection: `password authentication failed`

The container's data volume is from a previous run with different credentials. Wipe it:

```bash
docker volume rm ocpp_postgres_data
make compose-up
```

### ClickHouse: `DB::Exception: Memory limit exceeded`

The container's default memory limit is too low for batch ingestion. Raise it in `docker-compose.yml`:

```yaml
clickhouse:
  ulimits:
    nofile:
      soft: 262144
      hard: 262144
  deploy:
    resources:
      limits:
        memory: 4G
```

### k3d: `unable to import image — image not found`

Build the image locally first:

```bash
make build-image
```

Then re-import:

```bash
k3d image import eveys-ocpp:dev -c eveys-ocpp
```

### macOS firewall blocks docker-compose ports

System Settings → Network → Firewall → Options → allow `Docker` to accept incoming connections. Affects only LAN sharing scenarios — purely-local development is unaffected.

### `make compose-smoke` (or `pytest tests/e2e/`) fails on the WebSocket step

Two common causes:

1. **`eveys/ocpp` container hasn't finished starting.** Check `make compose-status`; wait for it to be `healthy`.
2. **Kafka topic isn't auto-created.** Pre-create with:

   ```bash
   docker compose -f deploy/compose/docker-compose.yml exec kafka \
       kafka-topics.sh --bootstrap-server localhost:9092 \
       --create --topic cp.boot --partitions 3 --replication-factor 1
   ```

---

## What this doc does NOT cover

- **Production / staging setup.** Cloud, registry, ingress, secrets, monitoring vendor — open decisions. Will land as ADRs and runbooks during Phases 5–6.
- **CI/CD pipeline internals.** See `.gitlab-ci.yml` and (future) `docs/08-cicd.md`.
- **Charger simulator usage.** Covered in `tests/e2e/README.md` (lands with task E1-13).
- **Performance tuning.** Each component (Postgres, Kafka, ClickHouse) has its own ops runbook; those land alongside the load-test work in Phase 4.

---

## Maintaining this doc

This doc is **part of the contract** for a working local-dev environment. When you change something that affects local-dev:

- Adding a new service (e.g. SchemaRegistry) → add a row to the service table, update the smoke test, add credentials.
- Bumping a major version (Postgres 16 → 17) → call out the migration in the section, link to upstream notes.
- Removing a service → remove from this doc *first*, then from compose.

A drift between this doc and reality counts as a Sev-2 bug. File it in GitLab Issues with label `docs:drift`.
