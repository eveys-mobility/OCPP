# Deploy to production

**Audience.** An operator about to run this in production.

**What this answers.** TLS at the charger edge, mTLS internally, autoscaling, secrets, graceful drain. Closes with a pre-flight sign-off checklist.

> If you're not yet running on a cluster, start with [`install.md`](./install.md). This page assumes the chart is installed and walks through what to harden before chargers hit it.

---

## 1. TLS at the charger edge

Chargers connect over `wss://` to Envoy. Envoy terminates TLS and forwards the cleartext WebSocket upstream to the gateway pod.

### 1.1 Get a certificate

Two reasonable paths:

- **cert-manager** with a public ACME issuer (Let's Encrypt, ZeroSSL) if your edge hostname is internet-resolvable.
- **A private CA** if your charger fleet trusts your own roots (common when chargers and the platform are operated by the same entity).

Either way, you end up with a Kubernetes `Secret` of type `kubernetes.io/tls` carrying `tls.crt` and `tls.key`. The chart references it by name:

```yaml
envoy:
  tls:
    secretName: eveys-ocpp-edge-tls
```

### 1.2 Confirm the SAN

The certificate's Subject Alternative Name **must** match the hostname the chargers are configured to dial. If a charger is configured for `wss://ocpp.example.com` and your cert is for `wss://gateway.example.com`, the TLS handshake fails and you'll see it in Envoy's access logs.

```bash
openssl x509 -in cert.crt -noout -text | grep -A1 "Subject Alternative Name"
```

### 1.3 Rotation

cert-manager renews on its own; the Envoy pods need to pick up the new Secret. The chart does *not* do automatic reload — the cleanest way is to roll the Envoy Deployment:

```bash
kubectl -n eveys-ocpp rollout restart deployment/eveys-ocpp-envoy
```

A few-second blip in new connections; existing sockets are unaffected. Schedule rotations off-peak.

---

## 2. mTLS between Envoy and the gateway

The chargers ↔ Envoy leg uses standard TLS (server cert; optional charger client cert). The Envoy ↔ gateway leg can be locked down with mutual TLS so anything other than Envoy hitting the gateway's WebSocket port fails the TLS handshake.

This is **off by default** in the chart. Turning it on is a two-side change:

```yaml
gateway:
  mtls:
    enabled: true
    secretName: eveys-ocpp-internal-mtls   # tls.crt, tls.key, ca.crt
envoy:
  upstreamMtls:
    secretName: eveys-ocpp-internal-mtls   # same Secret; Envoy presents the client side
```

The `ca.crt` in the Secret is what the gateway uses to verify Envoy's client cert; the `tls.crt`/`tls.key` is the server side. Use the same Secret on both Deployments so rotations stay in lockstep.

### 2.1 Why bother

In a single-namespace cluster, network policies are often considered sufficient. mTLS is the additional layer that means a compromised sidecar can't speak OCPP to your gateway pods even if it reaches the port. Recommended for any deployment that processes payments or runs in a shared cluster.

---

## 3. Bearer tokens and per-charger Basic Auth

Two auth boundaries on the gateway:

### 3.1 REST inbound (`EVEYS_OCPP_REST_INBOUND_TOKENS`)

CSV of acceptable bearer tokens. **At least one is required in production.** Tokens are validated constant-time.

Generate, store in your secrets manager, mount as the Helm Secret's `rest-inbound-tokens` key:

```bash
TOKEN=$(openssl rand -base64 48)
# put $TOKEN into the secrets manager
```

Rotation: the env var is a CSV, so two tokens coexist during the rollout window. Issue a new one, deploy, update your backend to use it, then remove the old one and deploy again.

### 3.2 Per-charger Basic Auth (`EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED`)

The WebSocket upgrade checks the charger's `Authorization: Basic <cp_id:password>` header against a bcrypt hash in `charge_point_credentials`. Two modes:

- **Permissive (default).** A charger with no credential row connects anyway. Migration shim — useful when onboarding a fleet that wasn't OCPP-secured before.
- **Strict.** A charger with no credential row, or with a wrong password, is rejected at the upgrade. **Production should set this to `true`.**

```yaml
gateway:
  basicAuth:
    required: true
```

Provision credentials for every charger first (see [`connect-a-charger.md`](./connect-a-charger.md) §2). Flip strict mode on once you've verified every charger is in the table.

---

## 4. Backend integration

The gateway calls *into* your backend for hot-path lookups (`Authorize`, session open/close) and pushes events to your webhook endpoints. Two env vars front this:

```bash
EVEYS_OCPP_BACKEND_BASE_URL=https://api.example.com
EVEYS_OCPP_BACKEND_TOKEN=<bearer token your backend accepts>
```

`backend-base-url` empty disables the hot path cleanly — the gateway falls back to the configured local behaviour. That's fine for dev; in production you almost certainly want the backend wired up so user authorization actually happens.

For webhooks:

```bash
EVEYS_OCPP_WEBHOOK_SECRET=<32+ bytes>
EVEYS_OCPP_WEBHOOK_URL_TX_STARTED=https://api.example.com/api/eveys/webhooks/tx.started
EVEYS_OCPP_WEBHOOK_ENABLE_TX_STARTED=true
# ... same pair for every event you want delivered
```

Rotation of the webhook secret follows the same overlap pattern as bearer tokens: your backend's verifier should accept either of two valid secrets for the rollover window.

---

## 5. Autoscaling

The gateway is built for horizontal scale. The chart ships `replicaCount: 2` as the floor; raise it as the fleet grows.

A common rule of thumb is one pod per ~2000 concurrent charger sockets, but the right number depends on CPU per pod and meter-sample volume. Let production traffic and the `eveys_ocpp_ws_connections_active` metric tell you.

### 5.1 HPA pattern

The chart does not ship an HPA — autoscaling target choice depends on your fleet. A workable shape:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: eveys-ocpp-gateway
  namespace: eveys-ocpp
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: eveys-ocpp-gateway
  minReplicas: 3
  maxReplicas: 30
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 60
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300        # don't thrash on minor dips
```

Scale-down is conservative on purpose: every pod that goes away takes its open WebSocket sockets with it; chargers reconnect, but you don't want to churn them every few minutes.

### 5.2 What *not* to autoscale on

Don't autoscale on Kafka-publish latency, on charger count, or on inbound-request rate — those are correlated with CPU but lag it. CPU utilisation tracks the actual workload most reliably.

---

## 6. Graceful drain

When a pod gets `SIGTERM`, it does three things, in order:

1. Flips `/api/v1/ready` to `503` so Envoy stops sending it new connections.
2. Continues to service existing sockets and inflight requests for up to `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` (default 25 s).
3. Closes the remaining sockets cleanly. Chargers reconnect to a healthy pod, usually within 1–3 seconds.

The chart's `terminationGracePeriodSeconds: 30` gives the kubelet a 5-second buffer past the gateway's grace window. If you raise the grace, raise the termination period in lockstep.

```yaml
gateway:
  terminationGracePeriodSeconds: 45        # if you raise it...
# and in env: EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS=40   # then also raise this
```

---

## 7. Database, Redis, Kafka, ClickHouse

These are platform-provided; the chart only references them by DSN. A few things to get right.

### 7.1 Postgres

- **Use a managed Postgres** (RDS / Cloud SQL / equivalent) unless you have a clear reason not to.
- **Enable connection pooling** between the gateway and Postgres (PgBouncer or your provider's built-in pooler). At fleet scale, raw connection counts explode.
- **The gateway expects schema version pinned to its app version.** Migrations run at pod start via Alembic; rolling back the gateway version may also require a Postgres downgrade. See [`upgrade.md`](./upgrade.md).

### 7.2 Redis

- The gateway uses Redis for the online registry, the cross-pod command bus, the Authorize cache, and the idempotency cache. All four are ephemeral by design.
- **Persistence is optional.** AOF / RDB doesn't add value here — losing Redis means chargers lose ~1 minute of online state and the registry rebuilds.
- Use a single Redis (or Redis Cluster) shared by all gateway pods. The online registry is the cross-pod state.

### 7.3 Kafka

- The gateway is a producer only.
- Topics are auto-created if your cluster allows it; otherwise create them ahead of time with retention configured to your audit needs.
- Production-grade settings: `acks=all`, `enable.idempotence=true` (the gateway does this — relevant for your consumer-side reasoning).

### 7.4 ClickHouse

- Used for time-series telemetry (meter values, status history).
- Sits behind the Kafka topics — a sidecar reads `cp.meter`, `cp.status`, etc. and writes batched inserts to ClickHouse. The gateway itself doesn't talk to ClickHouse synchronously.
- A few hundred million rows is comfortable; partition by month, drop old partitions per your retention policy.

---

## 8. Observability hooks

- **Prometheus.** Pods expose `/metrics` on port `9100`. Add a `ServiceMonitor` if you run kube-prometheus.
- **Logs.** Structured JSON to stdout. Configure your log shipper to forward; index on `cp_id`, `request_id`, `event`.
- **Traces.** OpenTelemetry; configure `EVEYS_OCPP_TRACING_OTLP_ENDPOINT` to your collector.
- **Sentry.** `EVEYS_OCPP_SENTRY_DSN` if you want exception capture.

Detailed metric and log catalogues in [`../reference/metrics.md`](../reference/metrics.md) and [`operate.md`](./operate.md).

---

## 9. The pre-flight sign-off checklist

Before chargers hit production traffic, every line must be ✅:

### TLS and auth

- [ ] WSS certificate installed and its SAN matches the hostname chargers are configured for.
- [ ] Internal mTLS (Envoy ↔ gateway) enabled.
- [ ] `EVEYS_OCPP_REST_INBOUND_TOKENS` set and tokens distributed to every caller.
- [ ] `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED=true` and every charger has a row in `charge_point_credentials`.
- [ ] Webhook signing secret set; backend verifies signatures.
- [ ] `EVEYS_OCPP_REST_OPENAPI_ENABLED=false` (or restricted to a private network).

### Capacity

- [ ] Minimum replica count ≥ 2; PDB `maxUnavailable: 1`.
- [ ] HPA wired up and tested with a load drill.
- [ ] Postgres pool sized for `(pod count × per-pod pool)`; PgBouncer in front if appropriate.

### Lifecycle

- [ ] `terminationGracePeriodSeconds` exceeds `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` by ≥ 5 s.
- [ ] Liveness probes the `/api/v1/health` endpoint; readiness probes `/api/v1/ready`.
- [ ] Rolling-update test passes — `kubectl rollout restart` and confirm zero charger drops on Prometheus.

### Observability

- [ ] Prometheus scraping the gateway and Envoy.
- [ ] Grafana dashboards loaded (the chart ships JSON dashboards under `deploy/grafana/dashboards/`).
- [ ] At least one alert wired: 5xx rate, `eveys_ocpp_ws_connections_active` cliff, Postgres-pool-saturation.
- [ ] Logs flowing to your aggregator with `cp_id` indexed.
- [ ] Traces flowing to a collector and sampled at a rate you can afford.

### Integration

- [ ] Backend `Authorize` endpoint responds within 200 ms p99 from inside the cluster.
- [ ] Webhook endpoints respond within their timeout, with idempotency on `event_id`.
- [ ] Kafka consumers have a `group_id` set and are consuming from `latest` (or `earliest` if you've already provisioned topics).

### Operational readiness

- [ ] Rollback documented and rehearsed. See [`upgrade.md`](./upgrade.md).
- [ ] On-call rota knows where the dashboards are.
- [ ] One run-through of [`operate.md`](./operate.md) §"common motions" with the on-call engineer.

When every box is ticked, you're ready.

---

## Where to go from here

- Day-2 operations: [`operate.md`](./operate.md).
- Security model: [`../concepts/security-model.md`](../concepts/security-model.md).
- Configuration reference: [`../reference/configuration.md`](../reference/configuration.md).
