# Operate

**Audience.** On-call engineers and platform operators.

**What this answers.** Health and readiness probes, structured logs, the metrics dashboard, how to drain a pod, how to roll back. The motions you do most often.

> First-time deployment lives in [`install.md`](./install.md) and [`deploy-to-production.md`](./deploy-to-production.md). This page is day-2.

---

## 1. Health and readiness

Two endpoints, two purposes.

### `/api/v1/health` — liveness

Answers "is this pod able to function at all?" Returns `200 OK` whenever the process is up and the load-bearing connections to dependencies (Postgres, Redis) work. Flips to `503` only when something downstream is genuinely sick.

Used by the Kubernetes **livenessProbe**. A red liveness means the pod is killed and rescheduled.

```bash
curl -s http://<pod-or-svc>:8080/api/v1/health
```

### `/api/v1/ready` — readiness / drain signal

Answers "should I receive new connections right now?" Returns `200 OK` normally. Returns `503` during graceful shutdown so the load balancer drains the pod cleanly.

Used by the Kubernetes **readinessProbe** *and* by Envoy's upstream health check.

```bash
curl -s http://<pod-or-svc>:8080/api/v1/ready
```

The two endpoints serve different goals. Liveness must stay green during drain; readiness must flip red. Don't conflate them.

---

## 2. Logs

Structured JSON to stdout, one record per line. Every log carries (at minimum) `level`, `event`, `request_id`. Context-rich logs add `cp_id`, `action` (the OCPP message name), `direction` (`rx`/`tx`).

### 2.1 Tail one charger

```bash
# kubectl
kubectl -n eveys-ocpp logs -l app=eveys-ocpp -f --since=10m | \
  jq 'select(.cp_id == "CP_X")'

# docker compose
docker logs -f eveys-ocpp 2>&1 | jq 'select(.cp_id == "CP_X")'
```

### 2.2 Grep by error

```bash
kubectl -n eveys-ocpp logs -l app=eveys-ocpp --since=1h | \
  jq 'select(.level == "error" or .level == "warning")'
```

### 2.3 Correlate one request across pods

When a backend call routes cross-pod, the `request_id` appears in two pods' logs — the pod that received the REST call and the pod that owned the charger's socket.

```bash
kubectl -n eveys-ocpp logs -l app=eveys-ocpp --since=10m | \
  jq 'select(.request_id == "8a3f2c40-3b8e-4d0a-9b62-7a3d5d1e9fa1")'
```

### 2.4 Don't grep for stack traces

The gateway treats expected errors (charger offline, validation failures, etc.) as logged warnings *without* a stack trace. Genuine bugs raise a `level=error` record with `exception` populated. Filter accordingly:

```bash
kubectl -n eveys-ocpp logs -l app=eveys-ocpp --since=1h | \
  jq 'select(.exception != null)'
```

If you see a stack trace, it's a bug worth filing.

---

## 3. Metrics

Pods expose Prometheus metrics on port `9100` at `/metrics`. The full catalogue is in [`../reference/metrics.md`](../reference/metrics.md). The headline four you should know by heart:

| Metric | What it tells you |
|---|---|
| `eveys_ocpp_ws_connections_active` | How many charger sockets are open right now, per pod. The fleet's pulse. |
| `eveys_ocpp_ocpp_handler_latency_seconds` | Inbound OCPP handling latency by action. p99 spikes here are the first sign something's slow. |
| `eveys_ocpp_grpc_request_latency_seconds` | Outbound dispatch latency. p99 spikes mean the WS leg or the charger side is slow. |
| `eveys_ocpp_handler_errors_total` | Per-action error count. Trend matters more than absolute. |

Dashboards under `deploy/grafana/dashboards/` cover fleet overview, per-pod, per-charger, reconnect storms, transactions, and the SLOs.

### Alert ideas (no specific recommendation; what's most actionable)

- 5xx rate on REST > 1% for 5m.
- `eveys_ocpp_ws_connections_active` drops > 20% over 1 minute (chargers got booted).
- Postgres-pool checkout latency > 100ms p99.
- `eveys_ocpp_handler_errors_total{action="StopTransaction"}` rate > 0 for 1 minute (financial path).

---

## 4. Common motions

### 4.1 Restart one pod

```bash
kubectl -n eveys-ocpp delete pod <pod-name>
```

Kubernetes recreates it; the readiness probe brings the new pod into rotation when it's ready. Existing connections on other pods are unaffected.

### 4.2 Rolling restart of every pod

```bash
kubectl -n eveys-ocpp rollout restart deployment/eveys-ocpp-gateway
```

Pods drain one at a time. Watch `kubectl rollout status`.

### 4.3 Drain a single pod for investigation

```bash
kubectl -n eveys-ocpp cordon <node-name>     # if the issue is a noisy node
# OR
kubectl -n eveys-ocpp delete pod <pod>       # gentler — just rotates one
```

The graceful drain mechanism keeps charging sessions intact: sockets stay on the pod for up to `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` while readiness goes red so Envoy stops sending new connections.

### 4.4 Force-evict a stuck charger

A charger that's misbehaving — keeps disconnecting, keeps re-sending bad payloads — can be forcibly evicted by deleting the credential row and waiting for it to disconnect:

```bash
# Strict mode (EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED=true) only.
psql "$DSN" -c "DELETE FROM charge_point_credentials \
                 WHERE charge_point_id = (SELECT id FROM charge_points \
                                          WHERE cp_id = 'CP_X')"
# Charger will fail its next WebSocket upgrade.
```

Re-provision the credential when whatever's wrong with the charger is fixed.

### 4.5 Replay a webhook

If a backend was down when an event was delivered and you need to re-send, the cleanest motion is to use the operator-queue endpoints (where available) or tail Kafka from a chosen offset. Kafka offsets are the source of truth for replay; webhooks are best-effort and not individually replayable through an API today.

---

## 5. Rolling back

A bad release is the day-2 hazard you'll actually face.

### 5.1 The fast path

```bash
helm rollback eveys-ocpp <previous-revision> --namespace eveys-ocpp
```

`<previous-revision>` is the revision number from `helm history eveys-ocpp -n eveys-ocpp` — typically the one immediately before the current.

This re-creates pods from the previous revision's manifests. Drain semantics apply just like a normal rolling update.

### 5.2 When the schema moved

If the bad release ran a forward-only Alembic migration, rolling the pods back without rolling the schema back is *unsafe*. Two options in order of preference:

1. **Roll forward** with a fix — fastest, lowest-risk for production data.
2. **Roll back the schema** if you absolutely must. Each migration has a `downgrade()` function; review it before running. `alembic downgrade -1` against the production DSN. Note this may discard data added by the new schema.

The release notes for each version call out whether the migration is reversible.

### 5.3 The damage-control playbook

If charger traffic is being impacted *right now*:

1. **Stop the bleed.** Roll the chart back to the previous revision.
2. **Get visibility.** Pull `kubectl logs` from the last 10 minutes, get a recent `/metrics` snapshot, write down what you saw. Future-you needs the artefacts.
3. **Confirm chargers recovered.** Watch `eveys_ocpp_ws_connections_active` climb back to the baseline.
4. **File against the gateway** with the artefacts from step 2.

Steps 1–3 take ≤ 5 minutes when rehearsed; step 4 is post-mortem material.

---

## 6. When things go wrong — symptom → likely cause

| Symptom | Look at | Likely cause |
|---|---|---|
| `503` from `/api/v1/ready` on every pod | Postgres reachability | DB outage. |
| `503` from `/api/v1/ready` on one pod | That pod's logs | Pod is draining (intentional) or it lost Redis. |
| Charger sockets dropping in waves | `eveys_ocpp_ws_connections_active` over time | Network blip; node restart; or a bad release rolling out. |
| REST returns `CHARGER_OFFLINE` for a charger you can ping | Online registry in Redis | Heartbeat staleness; the charger is connected but to a different process than you think. |
| `tx.stopped` not landing in Kafka | `eveys_ocpp_kafka_publish_total{outcome="failed"}` | Broker dropped the message; the synchronous REST close still happened. |
| Latency p99 climbed on `MeterValues` | `eveys_ocpp_db_query_latency_seconds` | Postgres slow; check pool saturation. |
| Webhook deliveries failing | Backend logs + `eveys_ocpp_webhook_attempts_total` by `outcome` | Backend rejecting or timing out; signature mismatch. |

When you don't recognise the symptom, the right first step is one minute of `kubectl logs` and one minute of `curl /metrics` — most causes show up immediately.

---

## Where to go from here

- Metric definitions: [`../reference/metrics.md`](../reference/metrics.md).
- Configuration knobs: [`../reference/configuration.md`](../reference/configuration.md).
- Version bumps: [`upgrade.md`](./upgrade.md).
