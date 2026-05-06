# Configuration reference

> **Source of truth:** `src/eveys_ocpp/settings.py`. Per [ADR-0025](./adr/0025-generated-config-reference.md),
> this page is **regenerated** from that file by
> `scripts/render_config_reference.py`. Do not hand-edit — change the
> Pydantic field instead and run `make config-export`.

Every variable below is read from the environment with prefix
`EVEYS_OCPP_` (e.g. `EVEYS_OCPP_LOG_LEVEL`). Defaults match
`Settings()` field defaults. Ranges come from the field's Pydantic
constraints (`ge=`, `le=`, `pattern=`) or `Literal[...]` alternatives.

**Stability column** answers "what happens if I change this":

- **tunable** — operator-facing, safe to change at runtime; no
  schema / wire-format consequences. Restart the service after the
  change so it picks the new value up.
- **structural** — changes how the service binds to the world.
  Coordinate with whoever else uses these ports / topics / DSNs.
- **dev-only** — local stacks and tests; do not set in production.

**Secret column** flags sensitive values; do not log, do not commit
to a values file, prefer your secrets manager. Phase 5 vault work
(E5-7) moves these to `SecretStr`; until then operators handle the
sensitivity.

---

## WS server

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_WS_HOST` | `0.0.0.0` | string | structural | no | Bind address for the OCPP WebSocket server. | Restricting from `0.0.0.0` (all interfaces) to a specific NIC limits which network reaches chargers. |
| `EVEYS_OCPP_WS_PORT` | `9000` | 1–65535 | structural | no | Port the WS server listens on. | Must match the docker-compose container port mapping and the charger's CSMS URL. Container exposes 9000 internally; host port may be remapped (e.g. 19000). |

## gRPC server

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_GRPC_HOST` | `0.0.0.0` | string | structural | no | Bind address for the inbound gRPC server (sibling services call into it for `RemoteStart`, `Reset`, etc.). | Same as WS_HOST: which NIC accepts gRPC. |
| `EVEYS_OCPP_GRPC_PORT` | `50051` | 1–65535 | structural | no | Port the gRPC server listens on. | All sibling services must agree on this; changing it requires a coordinated rollout. |

## REST server (ADR-0026, E3-7..E3-8)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_REST_ENABLED` | `true` | bool | structural | no | Whether to start the in-process REST server alongside WS and gRPC. Set to False in shapes that share this image but should not serve HTTP (e.g. the clickhouse-ingestor sidecar). | When False the gateway pod has no `/api/v1/*` surface; the backend cannot poll read state. |
| `EVEYS_OCPP_REST_HOST` | `0.0.0.0` | string | structural | no | Bind address for the inbound REST API (ADR-0026). | Restricting from `0.0.0.0` to a specific NIC limits which network reaches the backend-facing REST surface. |
| `EVEYS_OCPP_REST_PORT` | `8080` | 1–65535 | structural | no | Port the REST server listens on. | Production network policy must allow only the backend / operator UI to reach this port — distinct from the WS (9000) charger-facing port. |
| `EVEYS_OCPP_REST_INBOUND_TOKENS` | (empty) | string | tunable | **yes** | Comma-separated bearer tokens accepted on inbound REST requests. Multi-value to support rotation across consumers (eveys-backend, billing back-fill, operator UI). Each token must match exactly; whitespace is stripped. | Empty allowlist + `rest_auth_disabled=False` (the default) → all inbound requests are rejected with 401. Phase 5 vault work (E5-7) moves this to a SecretStr fetched at boot. |
| `EVEYS_OCPP_REST_AUTH_DISABLED` | `false` | bool | dev-only | no | Disable bearer-token validation entirely. Dev / laptop / unit-test convenience only — never set in production. | When True the gateway accepts any (or no) Authorization header on `/api/v1/*`. The boot-time log line `rest_auth.disabled=True` makes a forgotten flip obvious in any log review. |
| `EVEYS_OCPP_REST_DEFAULT_PAGE_SIZE` | `100` | 1–10000 | tunable | no | Default `limit` for cursor-paginated read endpoints. | Higher → fewer round-trips for the backend, more rows per response and per query. Lower → opposite. |
| `EVEYS_OCPP_REST_MAX_PAGE_SIZE` | `500` | 1–10000 | tunable | no | Hard cap on `limit` for cursor-paginated read endpoints. | Operators can lower this to defend against a misbehaving client requesting huge pages. The contract spec promises 1..500; raising past 500 is a contract change. |

## Kafka producer (ADR-0019)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_KAFKA_BROKERS` | `localhost:9092` | string | structural | no | Kafka bootstrap servers (comma-separated host:port). | Wrong broker → producer cannot start, gateway exits at boot. Inside the compose network use the INTERNAL listener (`kafka:29092`); from a laptop use `localhost:9092`. |
| `EVEYS_OCPP_KAFKA_ACKS` | `all` | `all` / `1` / `0` | tunable | no | Producer ack mode. `all` waits for full ISR (durable to leader crash); `1` only the leader; `0` fire-and-forget. | Lowering trades durability for latency. `tx.started` is on the financial path — never lower in production. ADR-0019. |
| `EVEYS_OCPP_KAFKA_ENABLE_IDEMPOTENCE` | `true` | bool | tunable | no | aiokafka producer-side dedup on retry. | Disabling lets a retried-after-lost-ack request duplicate. Pairs with E2-11's inbound replay dedup; both layers exist for defence in depth. |
| `EVEYS_OCPP_KAFKA_LINGER_MS` | `5` | 0–1000 | tunable | no | How long the producer waits to batch before sending (ms). | Lower → tighter `cp.meter` end-to-end latency, smaller batches, higher per-message overhead. Higher → bigger batches, more delay before billing-relevant `tx.started` lands. ADR-0019 § 'Per-topic linger'. |
| `EVEYS_OCPP_KAFKA_REQUEST_TIMEOUT_MS` | `30000` | 1000–120000 | tunable | no | How long a single produce request waits for the broker (ms). | Tighter than aiokafka's 40 s default so a stuck broker trips the handler's publish-failed log path quickly. Raising hides broker-stall incidents from observability. |
| `EVEYS_OCPP_KAFKA_RETRY_BACKOFF_MS` | `200` | 10–10000 | tunable | no | Wait between aiokafka retries on a recoverable error (ms). | Lower → faster recovery from transient broker blips, more load on a struggling broker. Higher → opposite. |

## Kafka topics

> The four topic names are part of the **frozen v1 contract** with downstream consumers (per `proto/events/v1/events.proto` and ADR-0018). Treat them as structural — renaming is an externally visible breaking change.

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_KAFKA_TOPIC_CP_METER` | `cp.meter` | string | structural | no | Firehose topic for `MeterValues`. ClickHouse ingestor consumes from here (E2-14). | Renaming detaches every existing consumer (ClickHouse ingestor, billing pipeline). |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_BOOT` | `cp.boot` | string | structural | no | `BootNotification` events. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_STATUS` | `cp.status` | string | structural | no | `StatusNotification` events. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_TX_STARTED` | `tx.started` | string | structural | no | `StartTransaction` events (financial path). | Renaming detaches every existing consumer. |

## Redis (online registry + pub/sub bus, ADR-0016)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_REDIS_URL` | `redis://localhost:6379/0` | string | structural | no | Single Redis client shared by registry, command bus, idempotency cache, Authorize cache. | Wrong DSN → gateway exits at boot. Compose uses `redis://redis:6379/0`. |
| `EVEYS_OCPP_REDIS_ONLINE_TTL_SECONDS` | `120` | 30–600 | tunable | no | TTL on `cp:online:{cp_id}` keys. Heartbeat refreshes the key; if the charger goes silent the key expires and the charger is considered offline. | 120 s aligns with OCPP 1.6 default heartbeat 60 s — gives ~2 missed heartbeats before declaring offline. Lower → quicker offline detection but more false positives on flaky links. |

## Postgres

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_DB_URL` | `postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp` | string | structural | **yes** | SQLAlchemy async DSN for the gateway's relational state (charge points, transactions, reservations, profiles). | Wrong DSN → gateway exits at boot. Schema changes go through Alembic — never edit the DB directly. The default carries the dev password; production DSNs always carry a real password and must be handled as a secret. |
| `EVEYS_OCPP_DB_POOL_SIZE` | `10` | 1–100 | tunable | no | SQLAlchemy connection-pool size per gateway pod. | Higher → more concurrent DB load capacity per pod, more idle connections. Total DB connections = `pool_size + max_overflow` x number of pods. |
| `EVEYS_OCPP_DB_MAX_OVERFLOW` | `20` | 0–200 | tunable | no | Extra connections allowed beyond pool size during bursts. | Set together with `DB_POOL_SIZE`. Postgres' `max_connections` ceiling is the hard limit. |

## Identity (Kubernetes downward-API)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_POD_ID` | hostname | string | structural | no | Identity of this pod for cross-pod routing. The Redis registry records 'charger X is held by pod Y'. | In Kubernetes set this from the downward API: `valueFrom: { fieldRef: { fieldPath: metadata.name } }`. Two pods with the same `pod_id` will fight over charger ownership. |

## Logging

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_LOG_LEVEL` | `INFO` | `DEBUG` / `INFO` / `WARNING` / `ERROR` / `CRITICAL` | tunable | no | Minimum log level emitted. | `DEBUG` produces several per-message lines per charger — high volume on a real fleet; use only briefly to investigate an incident. |
| `EVEYS_OCPP_LOG_JSON` | `true` | bool | tunable | no | Emit JSON logs (machine-readable) vs console (developer-readable). | Production sets `true` so the log aggregator parses fields. Local dev sets `false` for readability. |

## OCPP defaults

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` | `300` | 30–86400 | tunable | no | Sent back in `BootNotification.interval`; the charger pings us this often. | Lower → quicker offline detection at the cost of fleet-wide heartbeat traffic. Coordinate with `REDIS_ONLINE_TTL_SECONDS` (rule of thumb: TTL ~= 2x heartbeat). 300 s is the OCPP-recommended default. |

## Cross-pod command bus (ADR-0016)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_BUS_REQUEST_TIMEOUT_SECONDS` | `30` | 1–120 | tunable | no | How long a requesting pod waits for a cross-pod reply. | Defaults to the 30 s OCPP request ceiling — the bus shouldn't add headroom over the underlying call. Raising risks letting an OCPP RPC outlive the charger's own timeout. |

## Idempotency cache (E2-11)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_IDEMPOTENCY_TTL_SECONDS` | `300` | 30–3600 | tunable | no | Window for treating a repeat `(cp_id, message_id)` as a replay. | OCPP retry storms resolve within seconds; 5 min gives ample margin. Longer windows accumulate keys without benefit; OCPP message_ids are UUIDs and never reused across power cycles. |

## ClickHouse ingestion sidecar (ADR-0020)

> The ingestor is a separate process (`python -m eveys_ocpp.clickhouse.ingestor`); these settings configure it but the gateway itself does not connect to ClickHouse.

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_CLICKHOUSE_HOST` | `localhost` | string | structural | no | Where the ingestor + migrator find ClickHouse. | Compose uses `clickhouse`. |
| `EVEYS_OCPP_CLICKHOUSE_PORT` | `9000` | 1–65535 | structural | no | Native protocol port (8123 is HTTP, 9000 is native). The ingestor uses native; the migrator uses HTTP. | If you change this you must also update the migrator's `--port` if it differs from 8123. |
| `EVEYS_OCPP_CLICKHOUSE_DB` | `eveys_ocpp` | string | structural | no | ClickHouse database name. | Schema migrations target this DB; the migrator creates it on first run. |
| `EVEYS_OCPP_CLICKHOUSE_INGESTOR_GROUP` | `eveys-ocpp-clickhouse-ingestor` | string | structural | no | Kafka consumer-group ID for the ingestor. Multiple replicas share this group; Kafka rebalances partitions across them. | Renaming forces all consumers to re-read from the configured offset (typically earliest). |
| `EVEYS_OCPP_CLICKHOUSE_INGESTOR_BATCH_SIZE` | `500` | 1–10000 | tunable | no | Flush threshold in rows. | Lower → smaller batches, more INSERT round-trips, lower tail latency. Higher → opposite. ADR-0020 § 'Batch size vs latency'. |
| `EVEYS_OCPP_CLICKHOUSE_INGESTOR_BATCH_MAX_SECONDS` | `5.0` | 0.1–60.0 | tunable | no | Flush threshold in seconds (whichever-comes-first with `BATCH_SIZE`). | Lower → less worst-case ingestion delay; ClickHouse handles many small batches less efficiently than a few large ones. |

## Backend integration (ADR-0023, E3-2..E3-6)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_BACKEND_BASE_URL` | (empty) | string | structural | no | Base URL the gateway calls into. **Empty disables the backend client entirely** — handlers fall back to their offline policies (Authorize → Accepted, etc.). | Empty in dev. Production must set this; an empty value silently degrades to offline mode. |
| `EVEYS_OCPP_BACKEND_TOKEN` | (empty) | string | tunable | **yes** | Token used in `Authorization: Bearer ...` against the backend. | Move to vault in Phase 5 (E5-7). Until then handle as a secret and never commit a real value to .env or values.yaml. |
| `EVEYS_OCPP_BACKEND_TIMEOUT_AUTHORIZE_SECONDS` | `5.0` | 0.1–30.0 | tunable | no | HTTP timeout for the Authorize call (seconds). | Tighter values trip the gateway's offline fallback faster. The 30 s OCPP outer timeout is the hard ceiling. |
| `EVEYS_OCPP_BACKEND_TIMEOUT_SESSIONS_OPEN_SECONDS` | `8.0` | 0.1–30.0 | tunable | no | HTTP timeout for `POST /api/eveys/sessions/open` (StartTransaction). | Same shape as Authorize. |
| `EVEYS_OCPP_BACKEND_TIMEOUT_SESSIONS_CLOSE_SECONDS` | `10.0` | 0.1–30.0 | tunable | no | HTTP timeout for `POST /api/eveys/sessions/close` (StopTransaction). | StopTransaction tolerates a longer wait — closing a session is less time-critical than opening one. |
| `EVEYS_OCPP_BACKEND_TIMEOUT_DEFAULT_SECONDS` | `5.0` | 0.1–30.0 | tunable | no | Fallback timeout for any backend call without an explicit per-endpoint setting. | Used by `charge-points/register` and any future endpoint. |
| `EVEYS_OCPP_BACKEND_RETRY_ATTEMPTS_AUTHORIZE` | `1` | 0–5 | tunable | no | Retry attempts (excluding the first try) for Authorize. | Higher → resilience to transient blips, more latency on persistent outages. Authorize is on the OCPP hot path — keep low. |
| `EVEYS_OCPP_BACKEND_RETRY_ATTEMPTS_SESSIONS_OPEN` | `2` | 0–5 | tunable | no | Retry attempts for sessions/open. | StartTransaction is billing-critical; spending more retries here is the right trade. |
| `EVEYS_OCPP_BACKEND_RETRY_ATTEMPTS_SESSIONS_CLOSE` | `3` | 0–5 | tunable | no | Retry attempts for sessions/close. | The most important: a missed Close = a session that never billed. |
| `EVEYS_OCPP_BACKEND_CIRCUIT_BREAKER_THRESHOLD` | `5` | 1–100 | tunable | no | Open the breaker after this many consecutive failures. | Lower → quicker degradation to offline mode under outage, more flapping during transient incidents. |
| `EVEYS_OCPP_BACKEND_CIRCUIT_BREAKER_COOLDOWN_SECONDS` | `30.0` | 1.0–600.0 | tunable | no | How long the breaker stays open before letting one probe through (half-open). | Lower → faster recovery test but more load on a still-broken backend. |
| `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK` | `reject` | `reject` / `accept_offline` | tunable | no | What the Authorize handler returns when the backend is unreachable past the retry budget. `reject` → `Invalid` (safe). `accept_offline` → `Accepted` with a 5-min expiry (operator opt-in to un-billable risk). | ADR-0023 § 'Fallback policy'. Default `reject` is the safe billing-relevant choice. |
| `EVEYS_OCPP_BACKEND_REGISTER_FALLBACK` | `accept_offline` | `reject` / `accept_offline` | tunable | no | What BootNotification returns when the backend's `/charge-points/register` endpoint is unreachable past the retry budget. `accept_offline` → `Accepted` with the configured heartbeat interval (the local DB row anchors reconciliation when the backend recovers). `reject` → `Rejected`, charger stops calling. | Default `accept_offline` matches the contract's fail-soft model: a backend outage must not prevent chargers from booting and serving Authorize-cached sessions. Flip to `reject` only if the operator wants chargers offline during a backend incident. |

## Authorize cache (E3-4)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_BACKEND_AUTHORIZE_CACHE_ENABLED` | `true` | bool | tunable | no | Enable Redis caching of the Authorize result keyed on `(cp_id, id_tag)`. Cache hits short-circuit the backend round-trip on the OCPP hot path. | Disabling pushes every Authorize through the backend — useful for ops debugging when a stale cached `Blocked` is suspected. Re-enable as soon as the issue is understood. |
| `EVEYS_OCPP_BACKEND_AUTHORIZE_CACHE_TTL_SECONDS` | `30` | 1–3600 | tunable | no | TTL on cached Authorize entries. | Short enough that `Blocked`/`Expired` decisions propagate within ~30 s; long enough to absorb depot-shift bursts (a fleet returning at once = same-tag taps within a minute). Drop toward 1 s for ops debugging. |

## Outbound webhooks (E3-9)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_WEBHOOK_BASE_URL` | (empty) | string | tunable | no | Base URL the gateway POSTs webhook deliveries to. Empty disables the dispatcher entirely. Per-event URLs default to `<base>/<event-name>` and can be overridden individually. | Empty string = no webhook subsystem. Set to the backend's webhook receiver root URL to enable. |
| `EVEYS_OCPP_WEBHOOK_SECRET` | (empty) | string | tunable | **yes** | Shared HMAC-SHA-256 secret used to sign every webhook delivery. The backend's receiver verifies the `X-Eveys-Signature` header against the same secret. | Holds in vault. A leak lets an attacker forge delivery requests against the backend. Rotate via coordinated update (gateway + backend in lockstep). |
| `EVEYS_OCPP_WEBHOOK_URL_CP_BOOT` | (empty) | string | tunable | no | Override the URL for `cp.boot` events. Empty falls back to `<webhook_base_url>/cp-boot` when the base URL is set. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_ONLINE` | (empty) | string | tunable | no | Override the URL for `cp.online` events. Empty falls back to `<webhook_base_url>/cp-online`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_STATUS` | (empty) | string | tunable | no | Override the URL for `cp.status_changed` events. Empty falls back to `<webhook_base_url>/cp-status-changed`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_METER` | (empty) | string | tunable | no | Override the URL for `cp.meter` MeterValues events. Empty falls back to `<webhook_base_url>/cp-meter`. | MeterValues are high-volume — at 10k chargers this is ~333 webhooks/second. Off by default (see `webhook_enable_cp_meter`); prefer Kafka. |
| `EVEYS_OCPP_WEBHOOK_URL_TX_STARTED` | (empty) | string | tunable | no | Override the URL for `tx.started` events. Empty falls back to `<webhook_base_url>/tx-started`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_BOOT` | `true` | bool | tunable | no | Enable webhook delivery for `cp.boot` events. | Disable to silence boot-event pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_ONLINE` | `true` | bool | tunable | no | Enable webhook delivery for `cp.online` events (charger WebSocket connect). | Pairs with `cp.offline` for backend-side online-state tracking. No producer emits this yet — the WS server needs to be wired to publish `CpConnected` envelopes before this setting has an effect. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_STATUS` | `true` | bool | tunable | no | Enable webhook delivery for `cp.status_changed` events. | Disable to silence status-change pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_METER` | `false` | bool | tunable | no | Enable webhook delivery for `cp.meter` MeterValues events. **Off by default** — high volume. | Enabling this on a fleet >100 chargers will saturate the dispatcher's HTTP pool. Subscribe to Kafka instead. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_TX_STARTED` | `true` | bool | tunable | no | Enable webhook delivery for `tx.started` events. | Disable to silence transaction-start pushes. |
| `EVEYS_OCPP_WEBHOOK_CONSUMER_GROUP` | `eveys-ocpp-webhook-dispatcher` | string | structural | no | Kafka consumer group the webhook dispatcher uses to tail the four event topics. Distinct from the ClickHouse ingestor's group so the two pipelines run independently. | Changing this resets webhook consumer offsets. Keep stable unless intentionally replaying from earliest. |
| `EVEYS_OCPP_WEBHOOK_REQUEST_TIMEOUT_SECONDS` | `10.0` | 1.0–120.0 | tunable | no | HTTP timeout for a single webhook delivery attempt. Backend must respond within this window or the gateway treats the attempt as a transient failure and retries. | Lower = faster failure detection; higher = tolerates slower backends. 10 s matches the backend's documented response budget per `docs/integration/03-webhooks.md`. |
| `EVEYS_OCPP_WEBHOOK_MAX_ATTEMPTS` | `5` | 1–20 | tunable | no | Total delivery attempts before the gateway gives up and logs `webhook.delivery_failed`. Includes the first attempt. | Retries follow exponential backoff: 1 s, 5 s, 30 s, 2 min, 10 min for the default of 5. Lowering reduces the longest in-flight tail; raising tolerates longer backend outages. |

---

## Common operations

### "I want to read every variable on a running container."

```bash
docker exec eveys-ocpp env | grep '^EVEYS_OCPP_'
```

### "I want a starter `.env`."

`make config-export` regenerates `.env.example` alongside this file;
`cp .env.example .env` and edit. Secrets are blank in the example.

### "I changed a variable; do I need to redeploy?"

`Settings` is read once at process start (`get_settings()`). Yes —
restart the gateway pod (rolling restart in k8s) for a new value to
take effect. There is no live-reload path.

### "Which variables are sensitive?"

Anything tagged **secret = yes** in the tables above. Today that's
`BACKEND_TOKEN` and the password embedded in `DB_URL`. Phase 5 vault
work moves both to a secrets manager.

### "Where do I read the live values for an incident?"

`/health` will return the non-secret slice once E4-* (observability
phase) lands — until then, `docker exec ... env` is the answer.
