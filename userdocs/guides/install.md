# Install

**Audience.** A developer or operator bringing the stack up for the first time.

**What this answers.** Two paths — Docker Compose for development; Helm chart for Kubernetes. What's required, what's optional, how to verify the install is healthy.

> The fastest path is the Compose one in §1. The Helm path in §2 is the production shape. Pick by intent, not by capability — both bring up the same gateway.

---

## Prerequisites

| Tool | Version | Compose | Helm |
|---|---|---|---|
| Docker Desktop / Docker Engine | 4.30+ | ✅ | — |
| Docker Compose v2 | (bundled with Docker Desktop) | ✅ | — |
| `make` | any recent | ✅ | ✅ (helper targets) |
| `git` | 2.30+ | ✅ | ✅ |
| Python 3.13 | 3.13.x | optional, only for the simulator | — |
| [`uv`](https://docs.astral.sh/uv/) | 0.5+ | optional, only for the simulator | — |
| `kubectl` | 1.30+ | — | ✅ |
| `helm` | 3.15+ | — | ✅ |
| A Kubernetes cluster | 1.30+ | — | ✅ |

To confirm tools are installed:

```bash
docker --version && docker compose version && make --version && git --version
# plus, if you're going to Kubernetes:
kubectl version --client && helm version
```

The repository ships a single-command check too:

```bash
make doctor
```

It exits 0 when every required tool is present and reports exactly what's missing otherwise.

---

## 1. Local install with Docker Compose

The shortest path from `git clone` to a running gateway.

### 1.1 What you get

Six containers wired together by `deploy/compose/docker-compose.yml`:

| Container | What it is | Host port |
|---|---|---|
| `eveys-ocpp-postgres` | Postgres 16, schema migrations applied at start. | `5432` |
| `eveys-ocpp-redis` | Redis 7, online registry + command bus + caches. | `6379` |
| `eveys-ocpp-kafka` | Kafka (KRaft mode), event firehose. | `9092` |
| `eveys-ocpp-clickhouse` | ClickHouse for time-series telemetry. | `8123` (HTTP), `9000` (native, remapped to `9100` on the host so it doesn't clash with the gateway). |
| `eveys-ocpp` | The gateway pod. | WebSocket `19000`, REST `8080`, gRPC `50051`, Prometheus `9100`. |
| `eveys-ocpp-envoy` | Envoy with TLS termination + ring-hash. | `19443` (wss). |

The gateway is also fronted by the `clickhouse-ingestor` sidecar that tails Kafka and writes to ClickHouse.

### 1.2 Bring it up

```bash
git clone https://github.com/eveys-mobility/OCPP.git
cd OCPP
make compose-up
```

`make compose-up`:

1. Pulls or rebuilds the gateway image.
2. Starts every container in the right order.
3. Waits until the gateway answers `200 OK` on `/api/v1/ready`.
4. Runs database migrations (Postgres via Alembic; ClickHouse via the schema-migrations table).

When it exits 0, the stack is ready.

### 1.3 Verify it's healthy

```bash
# REST surface up?
curl -s http://localhost:8080/api/v1/health
# expected: {"status":"ok",...}

# Ready to accept charger connections?
curl -s http://localhost:8080/api/v1/ready
# expected: {"status":"ready",...}

# Swagger UI:
open http://localhost:8080/api/v1/docs

# Charger WebSocket reachable?
# (Cannot probe with curl alone; open ws://localhost:19000/<cp_id>
# with the OCPP subprotocol. The Quickstart shows how.)
```

### 1.4 Tear it down

```bash
make compose-down              # stops containers, keeps data volumes
make compose-down-volumes      # also wipes data — destructive
```

### 1.5 Common Compose tweaks

- **Need the metrics endpoint?** Visit `http://localhost:9100/metrics` while the stack is up.
- **Want to point at a real backend?** Set `EVEYS_OCPP_BACKEND_BASE_URL` in the gateway service's environment block; restart the container.
- **Need to see what's running?** `make compose-status` lists containers and their health.

---

## 2. Kubernetes install with Helm

For staging, production, or anything beyond a laptop.

### 2.1 What's in the chart

`deploy/helm/eveys-ocpp/` ships two workloads in a single release:

- **Gateway** — a `Deployment` with a headless `Service` so Envoy can hash to individual pod IPs. PodDisruptionBudget enforces `maxUnavailable: 0` during rolling updates so chargers always have a healthy pod to land on.
- **Envoy** — a `Deployment` with a `LoadBalancer` `Service` for the WSS edge. Ring-hashes inbound charger connections on `cp_id` to the gateway pods.

The chart does **not** ship Postgres, Redis, Kafka, ClickHouse — those are platform infrastructure and you provide them. Connection strings go into the chart values.

### 2.2 Prepare the cluster

Before installing the chart you need:

1. **The platform dependencies** reachable from the cluster — Postgres, Redis, Kafka, ClickHouse.
2. **A container registry** holding the gateway image. The repo ships a multi-stage `deploy/Dockerfile`; `make build-image` builds `eveys-ocpp:dev` locally. Push it to your registry with whatever tag you want to pin.
3. **A TLS certificate** for the WSS edge — either via cert-manager, external-secrets, or hand-rolled. The chart references a `Secret` by name; how it gets there is your platform's call.
4. **A bearer-token Secret** for the REST inbound auth — see §2.4.

### 2.3 Minimal values file

Create `eveys-ocpp.values.yaml` next to wherever you keep cluster manifests:

```yaml
gateway:
  image:
    repository: registry.example.com/eveys-mobility/ocpp
    tag: "0.1.0"
  replicaCount: 3

  config:
    logLevel: INFO
    logJson: true
    backendBaseUrl: https://api.example.com

  # Secret with these keys: db-url, backend-token, webhook-secret,
  # rest-inbound-tokens, sentry-dsn. Provisioned outside this chart.
  secrets:
    name: eveys-ocpp-secrets

envoy:
  service:
    type: LoadBalancer
  tls:
    secretName: eveys-ocpp-edge-tls    # the WSS cert Secret
```

### 2.4 The bearer-token Secret

The REST API uses bearer-token auth (`Authorization: Bearer <token>`). Tokens are a CSV in the gateway env var `EVEYS_OCPP_REST_INBOUND_TOKENS`, sourced from the Secret you named above.

Create one (or rotate one):

```bash
TOKEN=$(openssl rand -base64 32)
kubectl create secret generic eveys-ocpp-secrets \
  --from-literal=rest-inbound-tokens="$TOKEN" \
  --from-literal=db-url="postgresql+asyncpg://eveys:...@postgres.../eveys_ocpp" \
  --from-literal=backend-token="$BACKEND_TOKEN" \
  --from-literal=webhook-secret="$WEBHOOK_HMAC_SECRET" \
  --from-literal=sentry-dsn=""
```

Distribute the token to whoever calls the API — your backend service, your dashboards, your scripts.

### 2.5 Install

```bash
helm install eveys-ocpp ./deploy/helm/eveys-ocpp \
  --namespace eveys-ocpp \
  --create-namespace \
  --values eveys-ocpp.values.yaml
```

### 2.6 Verify the install

```bash
# Pods rolling out?
kubectl -n eveys-ocpp get pods

# Gateway healthy?
kubectl -n eveys-ocpp port-forward svc/eveys-ocpp-gateway 8080:8080
curl -s http://localhost:8080/api/v1/ready

# Envoy got an external IP?
kubectl -n eveys-ocpp get svc eveys-ocpp-envoy
```

A charger connects to `wss://<envoy-external-host>/<cp_id>` once DNS and the TLS cert are in place.

### 2.7 Upgrade and rollback

```bash
# Roll forward
helm upgrade eveys-ocpp ./deploy/helm/eveys-ocpp \
  --namespace eveys-ocpp \
  --values eveys-ocpp.values.yaml

# Roll back if it goes wrong
helm rollback eveys-ocpp 1 --namespace eveys-ocpp
```

The gateway is built for rolling updates — `maxUnavailable: 0`, `terminationGracePeriodSeconds: 30`, drain on `SIGTERM`. Active sockets stay on the old pods until those drain; new connections land on the new pods immediately.

Full upgrade semantics: [`upgrade.md`](./upgrade.md).

---

## 3. What's *not* in scope here

This guide stops at "the gateway is healthy and answering probes". Beyond that, two reads to queue up:

- **Connect a real charger** — [`connect-a-charger.md`](./connect-a-charger.md).
- **Go to production** — TLS, mTLS, autoscaling, secrets rotation, and the pre-flight checklist live in [`deploy-to-production.md`](./deploy-to-production.md).

If the install isn't healthy, the diagnostic moves are in [`operate.md`](./operate.md).

---

## Where to go from here

- Next steps once installed: [`connect-a-charger.md`](./connect-a-charger.md).
- Configuring further: [`../reference/configuration.md`](../reference/configuration.md).
- Operational concerns: [`operate.md`](./operate.md).
