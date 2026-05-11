# Configuration reference

**Use this if you** are tuning the gateway, debugging a misconfiguration, or building a Helm values file.

**Audience.** Operators and platform engineers.

**What this answers.** What environment variables exist, organised by what they control. Defaults, stability, and the variables you'll actually touch.

> The gateway exposes the live config under `GET /api/v1/admin/config` and the JSON schema under `GET /api/v1/sys/config/schema`. Both are auth-gated and useful when you want to introspect a running pod instead of reading docs.

---

## How configuration works

- Every setting is read from an environment variable prefixed `EVEYS_OCPP_`. Example: `EVEYS_OCPP_LOG_LEVEL=INFO`.
- The defaults are production-shaped where it doesn't cost anything; defaults that depend on the platform (DSNs, ports, secrets) require operator input.
- Settings are validated at startup. A misspelled value or an out-of-range integer fails the pod immediately with a clear error rather than starting in a confused state.
- Some settings are also **runtime-overridable** via `PATCH /api/v1/admin/config`. The allow-list lives in code; if you can't override a value at runtime, it's structural.

**Stability tiers** (what changing this does):

- **tunable** — safe to change at runtime; no wire-format or schema consequences. Restart the pod to pick up env-var changes (or use the admin endpoint for the runtime-overridable subset).
- **structural** — changes how the service binds to the world. Coordinate with whoever else uses the affected port / topic / DSN.
- **dev-only** — local stacks and tests. Do not set in production.

---

## Categories at a glance

| Category | What it controls | Examples |
|---|---|---|
| `identity` | What this pod calls itself. | `pod_id`, `service_name` |
| `logging` | Log level, format, where logs go. | `log_level`, `log_json` |
| `ws_server` | Charger-facing WebSocket binding and limits. | `ws_port`, `ws_basic_auth_required`, `ws_mtls_*` |
| `rest_server` | Backend-facing REST binding, auth, pagination. | `rest_port`, `rest_inbound_tokens`, `rest_default_page_size` |
| `grpc_server` | gRPC binding. | `grpc_port` |
| `auth` | Inbound bearer-token validation. | `rest_inbound_tokens` |
| `postgres` | Relational store. | `db_url`, pool sizes |
| `redis` | Online registry, command bus, caches. | `redis_url` |
| `kafka_producer` | Event firehose producer. | `kafka_brokers`, batch sizes, retries |
| `kafka_topics` | Topic names per event type. | `kafka_topic_tx_started`, etc. |
| `webhooks` | Webhook URLs, enable flags, signing secret, retry budget. | `webhook_url_tx_stopped`, `webhook_signing_secret` |
| `ocpp_defaults` | Charger-facing defaults (heartbeat interval, etc.). | `heartbeat_interval_seconds` |
| `backend_integration` | Hot-path REST calls to your backend. | `backend_base_url`, `backend_token`, fallback policy |
| `authorize_cache` | Redis-backed `Authorize` cache. | `authorize_cache_ttl_seconds` |
| `idempotency` | OCPP-replay dedup. | `idempotency_ttl_seconds` |
| `cross_pod_bus` | Redis pub/sub for command routing. | `bus_request_timeout_seconds` |
| `clickhouse_ingest` | Sidecar that tails Kafka to ClickHouse. | `clickhouse_url`, `clickhouse_batch_*` |
| `shutdown` | Graceful drain. | `shutdown_grace_period_seconds` |
| `metrics` | Prometheus surface. | `metrics_port` |
| `tracing` | OpenTelemetry. | `otlp_endpoint`, `tracing_sample_rate` |
| `sentry` | Exception capture. | `sentry_dsn` |

---

## The handful you'll actually touch

### Logging

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR`. Runtime-overridable. |
| `EVEYS_OCPP_LOG_JSON` | `true` | Set `false` for human-readable logs in dev. |

### REST and gRPC binding

| Variable | Default | Stability |
|---|---|---|
| `EVEYS_OCPP_REST_PORT` | `8080` | structural |
| `EVEYS_OCPP_GRPC_PORT` | `50051` | structural |
| `EVEYS_OCPP_METRICS_PORT` | `9100` | structural |
| `EVEYS_OCPP_REST_OPENAPI_ENABLED` | `true` | Disable for internet-exposed deployments. |
| `EVEYS_OCPP_REST_DEFAULT_PAGE_SIZE` | `100` | tunable |
| `EVEYS_OCPP_REST_MAX_PAGE_SIZE` | `1000` | tunable |
| `EVEYS_OCPP_REST_INBOUND_TOKENS` | — | **Required in prod.** CSV of bearer tokens. Secret. |

### WS / charger edge

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_WS_PORT` | `9000` | structural |
| `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED` | `false` | **Set `true` in production** once every charger has a credential row. |
| `EVEYS_OCPP_WS_MTLS_ENABLED` | `false` | Enable for Envoy ↔ gateway mTLS. |
| `EVEYS_OCPP_WS_MTLS_CERT_PATH` | — | Path to the server cert when mTLS is on. |
| `EVEYS_OCPP_WS_MTLS_KEY_PATH` | — | Path to the server key. |
| `EVEYS_OCPP_WS_MTLS_CA_PATH` | — | Path to the CA used to verify Envoy's client cert. |

### OCPP defaults

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` | `60` | Returned to the charger in the `BootNotification` reply. |

### Backend integration

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_BACKEND_BASE_URL` | — | Empty disables the hot path (gateway falls back to local behaviour). |
| `EVEYS_OCPP_BACKEND_TOKEN` | — | Bearer token your backend accepts. Secret. |
| `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK` | `reject` | What `Authorize` returns when the backend is unreachable: `reject` (deny) or `accept_offline` (allow). |

### Postgres

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_DB_URL` | — | **Required.** `postgresql+asyncpg://...`. Secret. |
| `EVEYS_OCPP_DB_POOL_SIZE` | `10` | tunable |
| `EVEYS_OCPP_DB_MAX_OVERFLOW` | `10` | tunable |

### Redis

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_REDIS_URL` | — | **Required.** `redis://...`. |

### Kafka

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_KAFKA_BROKERS` | — | CSV of bootstrap brokers. |
| `EVEYS_OCPP_KAFKA_TOPIC_*` | per-event default | See [`events.md`](./events.md). Renaming detaches every consumer. |

### Webhooks

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_WEBHOOK_SECRET` | — | **Required if any webhook is enabled.** Secret. |
| `EVEYS_OCPP_WEBHOOK_URL_*` | — | One per event (e.g. `WEBHOOK_URL_TX_STOPPED`). |
| `EVEYS_OCPP_WEBHOOK_ENABLE_*` | varies | Boolean toggle per event. Disabled = no deliveries. |

### Shutdown

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` | `25` | Must be < your `terminationGracePeriodSeconds` by ≥ 5 s. |

### Observability

| Variable | Default | Notes |
|---|---|---|
| `EVEYS_OCPP_TRACING_OTLP_ENDPOINT` | — | Empty disables OpenTelemetry. |
| `EVEYS_OCPP_TRACING_SAMPLE_RATE` | `0.01` | 1% by default; raise in dev/staging. |
| `EVEYS_OCPP_SENTRY_DSN` | — | Empty disables Sentry. |

---

## Secrets, not env vars

Five values are sensitive and **should never appear in plaintext** in a values file:

- `EVEYS_OCPP_REST_INBOUND_TOKENS`
- `EVEYS_OCPP_BACKEND_TOKEN`
- `EVEYS_OCPP_WEBHOOK_SECRET`
- `EVEYS_OCPP_DB_URL` (carries the DB password)
- `EVEYS_OCPP_SENTRY_DSN`

The Helm chart references them by Secret name; your secrets manager (Vault, external-secrets, sealed-secrets, AWS Secrets Manager, etc.) is responsible for getting them onto the cluster.

---

## Runtime overrides via the admin API

A bounded set of `tunable` settings can be changed without restarting the pod. The allow-list is small on purpose — anything that affects wire formats, ports, or persistence is *not* in it.

Typical safe overrides:

- `EVEYS_OCPP_LOG_LEVEL` (drop to `DEBUG` for an investigation, raise back to `INFO`)
- `EVEYS_OCPP_TRACING_SAMPLE_RATE`
- `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK`
- `EVEYS_OCPP_WEBHOOK_URL_*` / `EVEYS_OCPP_WEBHOOK_ENABLE_*`

```bash
# Drop to DEBUG on one pod for ten minutes
curl -s -X PATCH http://<pod>:8080/api/v1/admin/config \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"key":"EVEYS_OCPP_LOG_LEVEL","value":"DEBUG"}'

# Restore default
curl -s -X DELETE http://<pod>:8080/api/v1/admin/config/overrides/EVEYS_OCPP_LOG_LEVEL \
  -H "Authorization: Bearer $TOKEN"
```

The override persists in Redis so a new pod inherits it; remove the override to revert. Pods broadcast override changes to each other via the cross-pod bus.

---

## Looking up the live config

```bash
curl -s http://<gateway>/api/v1/admin/config \
  -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
```

Secrets are redacted in the response (shown as `***`). The full JSON schema for the config model is at `GET /api/v1/sys/config/schema` — useful for building admin UIs.

---

## Where to go from here

- The metrics surface: [`metrics.md`](./metrics.md).
- Operational use of these knobs: [`../guides/operate.md`](../guides/operate.md).
- Production hardening: [`../guides/deploy-to-production.md`](../guides/deploy-to-production.md).
