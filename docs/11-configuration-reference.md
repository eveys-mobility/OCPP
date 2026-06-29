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
| `EVEYS_OCPP_WS_RATE_LIMIT_ENABLED` | `true` | bool | tunable | no | Per-charger inbound-CALL rate limiter (E5-3). When True, each charger's CALLs are checked against a Redis-backed token bucket; overrun drops the message silently and bumps `eveys_ocpp_rate_limit_throttled_total{action}`. Kill-switch for emergencies; on a Redis blip the limiter already fails open, so flipping this to False should rarely be needed. | Disabling removes the per-charger DoS protection — a single misbehaving charger can saturate handler / Postgres / Kafka work for the whole pod. |
| `EVEYS_OCPP_WS_RATE_LIMIT_CAPACITY` | `30` | 1–10000 | tunable | no | Token-bucket capacity per charger — the burst allowance. First N CALLs in a quiet period all pass through; refill tops the bucket up over time. Default 30 absorbs reconnect bursts (BootNotification + StatusNotifications) without throttling normal chargers. | Lower → tighter burst tolerance, more throttles on reconnect storms. Higher → larger spike a single charger can land on us before the cap kicks in. |
| `EVEYS_OCPP_WS_RATE_LIMIT_REFILL_PER_SECOND` | `1.0` | <= 1000.0 | tunable | no | Token-bucket refill rate per charger (tokens per second). Default 1.0 = sustained 60 CALLs/min, well above any normal OCPP 1.6 charger's steady-state traffic. Pair with the capacity field: bucket caps at `ws_rate_limit_capacity`. | Lower → tighter steady-state cap. Higher → looser (closer to no-limit). Don't set above ~10 unless a specific vendor's traffic profile is documented to exceed 600 CALLs/min. |

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
| `EVEYS_OCPP_REST_DEFAULT_PAGE_SIZE` | `100` | 1–10000 | tunable | no | Default `limit` for cursor-paginated read endpoints. | Higher → fewer round-trips for the backend, more rows per response and per query. Lower → opposite. |
| `EVEYS_OCPP_REST_MAX_PAGE_SIZE` | `500` | 1–10000 | tunable | no | Hard cap on `limit` for cursor-paginated read endpoints. | Operators can lower this to defend against a misbehaving client requesting huge pages. The contract spec promises 1..500; raising past 500 is a contract change. |
| `EVEYS_OCPP_REST_OPENAPI_ENABLED` | `false` | bool | tunable | no | Mount OpenAPI schema + Swagger UI + ReDoc on the gateway REST surface. Default False per ADR-0026 — the gateway does not self-publish a discoverable schema in production. Operators flip this to True in dev / staging / behind a VPN to get a clickable spec at `/api/v1/docs`. | When True the gateway serves three new paths under the REST port: `/api/v1/openapi.json`, `/api/v1/docs` (Swagger UI), and `/api/v1/redoc`. Auth still applies to these paths — only token-bearers can read the spec. A boot-time WARNING log makes a forgotten flip obvious in any log review. The static spec at `docs/api/openapi.yaml` is regenerated by `make openapi-export` and is the canonical artifact for sharing with backend teams / Postman. |
| `EVEYS_OCPP_ADMIN_RESTART_ENABLED` | `false` | bool | tunable | no | Allow `POST /api/v1/admin/restart` to terminate the process so the container's restart policy respawns it. Default False — even with the admin token, the endpoint returns 503 until an operator opts in by setting this. Operators wire this to the Console UI's restart button when they want config-key changes that need a process restart (Kafka topic, port, JWT secret) to be drivable from the browser instead of via SSH. | When True, a token-bearing caller can shut the gateway down at will. Cluster impact depends on the supervisor: under Docker Compose with restart: unless-stopped the process comes back within seconds; under k8s the Deployment respawns the pod. In-flight chargers see a clean WS close via the existing drain in `shutdown.py` before the process exits. A 500ms gap between the 202 response and the actual SIGTERM gives the Console UI's overlay time to start polling `/api/v1/health`. |
| `EVEYS_OCPP_ADMIN_RESTART_DEBOUNCE_SECONDS` | `5.0` | float | tunable | no | Minimum gap between accepted restart requests. A second POST inside this window returns 202 but does NOT schedule another exit — guards against double-clicks and the Console UI's overlay racing the operator's button. | Per-pod, in-memory. Not persisted across restarts. |
| `EVEYS_OCPP_SSE_ENABLED` | `false` | bool | tunable | no | Mount the Server-Sent Events endpoint `GET /api/v1/charge-points/{cp_id}/events`. Off by default so pods that never serve operator UIs don't open the per-pod Kafka consumer the SSE bus uses. Flip on for any pod that front-ends the Console / operator REST surface. | When True the gateway starts one extra AIOKafkaConsumer subscribed to the seven topics keyed by `cp_id`. The consumer's group_id is randomized per pod (non-durable, auto_offset_reset='latest'): SSE tails from now, never replays history. Off pods stay zero-cost. |
| `EVEYS_OCPP_SSE_KAFKA_GROUP_PREFIX` | `eveys-ocpp-sse` | string | tunable | no | Prefix for the per-pod Kafka consumer-group id used by the SSE bus. The full id is `<prefix>-<pod_id>-<random>` so each pod gets its own group and no rebalances happen across pods. | Renaming detaches the in-flight SSE consumers on rollout and they start at `latest`, which is correct — tail-from-now is the spec. Don't share this prefix with the ClickHouse ingestor's group; the ingestor needs durable, shared offsets. |
| `EVEYS_OCPP_SSE_QUEUE_MAX_SIZE` | `256` | 1–10000 | tunable | no | Bounded queue size per SSE subscriber. When a slow client fills its queue, the bus drops that subscriber's stream (closes with an `error` event) rather than block the Kafka consumer and stall every other subscriber on the pod. | Smaller queue → faster drop of slow clients, less RAM. Larger queue → more tolerance for brief operator-side pauses (browser tab backgrounding, GC). 256 holds ~10s of MeterValues at the busiest realistic CP rate. |
| `EVEYS_OCPP_SSE_HEARTBEAT_SECONDS` | `20.0` | <= 300.0 | tunable | no | Idle-stream heartbeat interval. The SSE endpoint emits a comment line (`:\n\n`) at this cadence so intermediate proxies don't close an idle connection. | Lower → more keep-alive traffic, faster proxy-drop detection. Higher → less traffic, longer time-to-detect a dead intermediate. Envoy's idle timeout is 1h by default; the 20s default sits well inside it. |

## Inbound auth (REST bearer + WS basic-auth + mTLS)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_REST_INBOUND_TOKENS` | (empty) | — | tunable | **yes** | Comma-separated bearer tokens accepted on inbound REST requests. Multi-value to support rotation across consumers (eveys-backend, billing back-fill, operator UI). Each token must match exactly; whitespace is stripped. | Empty allowlist + `rest_auth_disabled=False` (the default) → all inbound requests are rejected with 401. Stored as `SecretStr` (E5-7) so a stray `print(settings)` or unstructured-log dump shows the redacted placeholder; call `.get_secret_value()` at the explicit point of use. |
| `EVEYS_OCPP_REST_AUTH_DISABLED` | `false` | bool | dev-only | no | Disable bearer-token validation entirely. Dev / laptop / unit-test convenience only — never set in production. | When True the gateway accepts any (or no) Authorization header on `/api/v1/*`. The boot-time log line `rest_auth.disabled=True` makes a forgotten flip obvious in any log review. |
| `EVEYS_OCPP_WS_MTLS_ENABLED` | `false` | bool | structural | no | When True, the WS server requires client TLS authentication (`ssl.CERT_REQUIRED`) on inbound connections. The peer must present a certificate signed by the CA at `ws_mtls_ca_path`. Used in production to authenticate the Envoy → gateway leg (E5-5, ADR-0011); off in compose dev because charger sims don't carry certs. | Enabling without setting cert / key / ca paths fails loud at boot. Disabling in production drops the in-cluster authentication boundary — the WS server then trusts whatever can reach `:9000`. |
| `EVEYS_OCPP_WS_MTLS_CERT_PATH` | (empty) | string | structural | no | Filesystem path to the gateway's server certificate (PEM). Loaded into the WS server's `SSLContext` when `ws_mtls_enabled=True`. In k8s the operator mounts this from a TLS Secret via the Helm chart. | Wrong path → boot fails with `FileNotFoundError`. The cert is the gateway's own identity to Envoy. |
| `EVEYS_OCPP_WS_MTLS_KEY_PATH` | (empty) | string | structural | no | Filesystem path to the gateway's server private key (PEM). Loaded with `ws_mtls_cert_path` into the `SSLContext`. | Path leak isn't a secret leak — the file at the path is. File permissions on the mount are the operator's concern; the gateway just `open()`s it once at boot. |
| `EVEYS_OCPP_WS_MTLS_CA_PATH` | (empty) | string | structural | no | Filesystem path to the CA bundle (PEM) used to verify the client cert Envoy presents on each upstream connection. Anything signed by this CA is trusted; rotate the bundle to revoke. | Trust anchor for the Envoy-side identity. A widened CA (e.g. a public root) effectively disables the auth boundary. Mount it as a tightly-scoped private CA, not a public one. |
| `EVEYS_OCPP_AUTH_PENDING_GRACE_SECONDS` | `180` | 10–3600 | tunable | no | Window (in seconds) granted to a charger whose authorization is `pending` — first-seen devices and devices an operator has not yet decided on. The WS upgrade is accepted so an operator can see vendor / model / serial from the BootNotification while deciding, but a force-disconnect timer starts at upgrade time. If the operator has not posted `/authorizations/{cp_id}/approve` by the deadline the WS is closed with code 1008 and subsequent upgrade attempts are rejected with 401 until a decision is made. | Too short and a slow operator misses the window; too long and an unapproved charger sends OCPP traffic for minutes. 180 s (the default) is the documented trade-off in the device-authorization design. |
| `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED` | `false` | bool | tunable | no | WS-edge Basic Auth (E5-6) is always *attempted* — every upgrade is checked against `charge_point_credentials`. This flag controls behaviour for chargers that have **no credential row** yet: when False (default) those chargers are accepted, which lets a fleet migrate gradually; when True the upgrade is rejected with 401 and the operator must provision a credential before the charger connects. | Production sets True so an unprovisioned charger can't sneak through. Dev / compose stays False so the simulator (which doesn't carry creds) keeps working. |

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
| `EVEYS_OCPP_KAFKA_TOPIC_CP_CONNECTED` | `cp.connected` | string | structural | no | WS-connect events. Source for the `cp.online` webhook. Published by the WS server immediately after the registry marks the charger online. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_DISCONNECTED` | `cp.disconnected` | string | structural | no | WS-disconnect events. Source for the `cp.offline` webhook. Published by the WS server only when the registry's compare-and-delete confirms we still owned the key (so a reconnect-to-different-pod race never produces a spurious offline event). | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_OFFLINE_DURATION` | `cp.offline_duration` | string | structural | no | Per-CP offline-duration events. Emitted on reconnect when the gateway finds the marker its prior disconnect left in Redis; carries went_offline_at, came_online_at and offline_seconds. ClickHouse `cp_offline_duration` is ingested from this topic. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_HEARTBEAT` | `cp.heartbeat` | string | structural | no | OCPP `Heartbeat` events. Source for the `cp.heartbeat` webhook the backend uses to keep `station_devices.last_online` fresh between WS connect / disconnect lifecycle events. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_METER` | `cp.meter` | string | structural | no | Firehose topic for `MeterValues`. ClickHouse ingestor consumes from here (E2-14). | Renaming detaches every existing consumer (ClickHouse ingestor, billing pipeline). |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_BOOT` | `cp.boot` | string | structural | no | `BootNotification` events. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_STATUS` | `cp.status` | string | structural | no | `StatusNotification` events. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_FIRMWARE_STATUS` | `cp.firmware_status` | string | structural | no | `FirmwareStatusNotification` events. Source for the `cp.firmware_status_changed` webhook. Low volume (a few per charger per firmware-update lifecycle). | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_DIAGNOSTICS_STATUS` | `cp.diagnostics_status` | string | structural | no | `DiagnosticsStatusNotification` events. Source for the `cp.diagnostics_status_changed` webhook. Low volume (a few per charger per diagnostics-upload lifecycle). | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_TX_STARTED` | `tx.started` | string | structural | no | `StartTransaction` events (financial path). | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_TX_STOPPED` | `tx.stopped` | string | structural | no | `StopTransaction` events emitted after a successful DB commit. Belt-and-braces signal alongside the synchronous `/sessions/close` REST call — see `docs/integration/03-webhooks.md`. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_SECURITY_EVENT` | `cp.security_event` | string | structural | no | `SecurityEventNotification` events from chargers (OCPP 1.6 Security Whitepaper §4). Audit-grade; downstream SIEM consumers tail this for alerting on invalid signatures, cert tampering, etc. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_CREDENTIAL_ROTATED` | `cp.credential_rotated` | string | structural | no | Per-charger Basic Auth credential rotations (TC_073). Emitted when an operator sets, rotates, or removes a charger's password via the REST surface. Audit-grade; SIEM consumers tail this alongside `cp.security_event`. The password is never carried in the payload. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_CSR_SUBMITTED` | `cp.csr_submitted` | string | structural | no | `SignCertificate` CSRs from chargers (OCPP 1.6 Security Whitepaper §4.13). Operator review hook — the gateway persists each CSR to `pending_certificate_signings` and publishes here so external systems can observe pending work. The actual signing pipeline is a separate concern. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_TOPIC_CP_OCPP_FRAMES` | `cp.ocpp_frames` | string | structural | no | Every OCPP frame on the WS, both directions, keyed by `cp_id`. Inbound (CP → gateway) frames are published just after `unpack()` succeeds; outbound (gateway → CP) frames are published just before they hit the WebSocket. The raw JSON the wire actually carried is included verbatim. Best-effort: publish failures are logged and counted but never block the WS path. Volume is high — the Console's NDJSON event log is the intended consumer, not the ClickHouse Kafka-engine table. | Renaming detaches every existing consumer. |
| `EVEYS_OCPP_KAFKA_PUBLISH_OCPP_FRAMES` | `true` | bool | tunable | no | Master switch for the cp.ocpp_frames publish. Disable to stop publishing raw frames without rebuilding (e.g. during an incident where the topic is overwhelming a downstream consumer). The existing digest topics — cp.boot, cp.status, cp.meter, tx.started — are unaffected. | Disables the audit log; downstream gaps until re-enabled. |

## Redis (online registry + pub/sub bus, ADR-0016)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_REDIS_URL` | `redis://localhost:6379/0` | string | structural | no | Single Redis client shared by registry, command bus, idempotency cache, Authorize cache. | Wrong DSN → gateway exits at boot. Compose uses `redis://redis:6379/0`. |
| `EVEYS_OCPP_REDIS_ONLINE_TTL_SECONDS` | `120` | 30–600 | tunable | no | TTL on `cp:online:{cp_id}` keys. Heartbeat refreshes the key; if the charger goes silent the key expires and the charger is considered offline. | 120 s aligns with OCPP 1.6 default heartbeat 60 s — gives ~2 missed heartbeats before declaring offline. Lower → quicker offline detection but more false positives on flaky links. |

## Postgres

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_DB_URL` | (secret; redacted) | — | structural | **yes** | SQLAlchemy async DSN for the gateway's relational state (charge points, transactions, reservations, profiles). Stored as `SecretStr` (E5-7) — the embedded password never appears in `repr(settings)` / log dumps. | Wrong DSN → gateway exits at boot. Schema changes go through Alembic — never edit the DB directly. The default carries the dev password; production DSNs always carry a real password and must be handled as a secret. |
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
| `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` | `60` | 30–86400 | tunable | no | Sent back in `BootNotification.interval`; the charger pings us this often. | Lower → quicker offline detection at the cost of fleet-wide heartbeat traffic. Coordinate with `REDIS_ONLINE_TTL_SECONDS` (rule of thumb: TTL ~= 2x heartbeat). 60 s pairs with the default 120 s online TTL — 2 missed heartbeats triggers offline detection. The OCPP-1.6 spec doesn't mandate a value; 300 s was the previous gateway default. |
| `EVEYS_OCPP_METER_VALUE_SAMPLE_INTERVAL_SECONDS` | `15` | 5–3600 | tunable | no | Value pushed to every charger after `BootNotification.Accepted` via `ChangeConfiguration(MeterValueSampleInterval=…)`. Sets how often the charger sends `MeterValues` during a transaction. | Lower → finer-grained power/energy charts on the operator console, more `MeterValues` frames per session (and more Kafka/ClickHouse traffic). 15 s is fine for AC sessions on the order of hours; consider 5-10 s for short fast-charge sessions if you want sub-minute resolution. Pushed best-effort: a charger that rejects the configuration key is logged but does not fail boot. See https://github.com/eveys-mobility/OCPP/issues/238. |

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
| `EVEYS_OCPP_CLICKHOUSE_INGESTOR_MAX_FLUSH_FAILURES` | `10` | 1–10000 | tunable | no | Consecutive INSERT failures before the ingestor exits non-zero so the supervisor (docker compose, kubernetes) restarts it. Without this the process loops forever on a wedged pipeline (wrong CH instance, missing schema, type mismatch) and silently drops fresh events while the Kafka consumer-group offset never advances. | Lower → faster CrashLoopBackOff signal at the cost of tolerating fewer transient blips. Higher → more patience for a flaky CH at the cost of a longer dead window before the operator finds out. |

## Backend integration (ADR-0023, E3-2..E3-6)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_BACKEND_BASE_URL` | (empty) | string | structural | no | Base URL the gateway calls into. **Empty disables the backend client entirely** — handlers fall back to their offline policies (Authorize → Accepted, etc.). | Empty in dev. Production must set this; an empty value silently degrades to offline mode. |
| `EVEYS_OCPP_BACKEND_TOKEN` | (empty) | — | tunable | **yes** | Token used in `Authorization: Bearer ...` against the backend. Stored as `SecretStr` (E5-7) — call `.get_secret_value()` at the HTTP client boundary; never log. | Vault provisioning lands with the Helm chart in E5-1; until then the operator handles `EVEYS_OCPP_BACKEND_TOKEN` via the platform's existing secret-management story (k8s Secret, AWS Secrets Manager, etc.). |
| `EVEYS_OCPP_OUTBOUND_TLS_VERIFY` | `true` | bool | tunable | no | Whether to verify TLS certificates on every outbound connection the gateway makes — both the backend HTTP client (Authorize / sessions/open / sessions/close / charge-points/register) and the webhook dispatcher. Default True for production. Local dev with a self-signed cert (e.g. https://toger.test) sets this to False so the gateway doesn't slam the circuit breaker on every Authorize and the webhook delivery doesn't fail every attempt. Setting False in production silently disables a real security control — boot logs a loud warning to make that obvious in case it ever ships by accident. | False allows MITM against the backend AND webhook legs. Acceptable for local dev; never in production. Phase 5 vault work (E5-7) will swap this for proper CA-bundle config per leg. |
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
| `EVEYS_OCPP_WEBHOOK_SECRET` | (empty) | — | tunable | **yes** | Shared HMAC-SHA-256 secret used to sign every webhook delivery. The backend's receiver verifies the `X-Eveys-Signature` header against the same secret. Stored as `SecretStr` (E5-7); the dispatcher calls `.get_secret_value()` at the signing boundary. | Holds in vault. A leak lets an attacker forge delivery requests against the backend. Rotate via coordinated update (gateway + backend in lockstep). |
| `EVEYS_OCPP_WEBHOOK_URL_CP_BOOT` | (empty) | string | tunable | no | Override the URL for `cp.boot` events. Empty falls back to `<webhook_base_url>/cp-boot` when the base URL is set. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_FIRMWARE_STATUS` | (empty) | string | tunable | no | Override the URL for `cp.firmware_status_changed` events. Empty falls back to `<webhook_base_url>/cp-firmware-status-changed`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_DIAGNOSTICS_STATUS` | (empty) | string | tunable | no | Override the URL for `cp.diagnostics_status_changed` events. Empty falls back to `<webhook_base_url>/cp-diagnostics-status-changed`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_OFFLINE` | (empty) | string | tunable | no | Override the URL for `cp.offline` events. Empty falls back to `<webhook_base_url>/cp-offline`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_ONLINE` | (empty) | string | tunable | no | Override the URL for `cp.online` events. Empty falls back to `<webhook_base_url>/cp-online`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_STATUS` | (empty) | string | tunable | no | Override the URL for `cp.status_changed` events. Empty falls back to `<webhook_base_url>/cp-status-changed`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_HEARTBEAT` | (empty) | string | tunable | no | Override the URL for `cp.heartbeat` events. Empty falls back to `<webhook_base_url>/cp-heartbeat`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_CP_METER` | (empty) | string | tunable | no | Override the URL for `cp.meter` MeterValues events. Empty falls back to `<webhook_base_url>/cp-meter`. | MeterValues are high-volume — at 10k chargers this is ~333 webhooks/second. Off by default (see `webhook_enable_cp_meter`); prefer Kafka. |
| `EVEYS_OCPP_WEBHOOK_URL_TX_STARTED` | (empty) | string | tunable | no | Override the URL for `tx.started` events. Empty falls back to `<webhook_base_url>/tx-started`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_URL_TX_STOPPED` | (empty) | string | tunable | no | Override the URL for `tx.stopped` events. Empty falls back to `<webhook_base_url>/tx-stopped`. | Per-event routing override. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_BOOT` | `true` | bool | tunable | no | Enable webhook delivery for `cp.boot` events. | Disable to silence boot-event pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_ONLINE` | `true` | bool | tunable | no | Enable webhook delivery for `cp.online` events (charger WebSocket connect). | Pairs with `cp.offline` for backend-side online-state tracking. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_FIRMWARE_STATUS` | `true` | bool | tunable | no | Enable webhook delivery for `cp.firmware_status_changed` events (charger-reported firmware-update state-machine transitions). Low volume — a few events per charger per firmware-update lifecycle. | Disable to silence firmware-status pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_DIAGNOSTICS_STATUS` | `true` | bool | tunable | no | Enable webhook delivery for `cp.diagnostics_status_changed` events (charger-reported diagnostics-upload state-machine transitions). Low volume — a few events per charger per diagnostics-upload lifecycle. | Disable to silence diagnostics-status pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_OFFLINE` | `true` | bool | tunable | no | Enable webhook delivery for `cp.offline` events (charger WebSocket disconnect — only fired when this pod still owned the registry key, so a reconnect-to-different-pod race never produces a spurious offline event). | Pairs with `cp.online` for backend-side online-state tracking. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_STATUS` | `true` | bool | tunable | no | Enable webhook delivery for `cp.status_changed` events. | Disable to silence status-change pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_HEARTBEAT` | `true` | bool | tunable | no | Enable webhook delivery for `cp.heartbeat` events. Drives the backend's `station_devices.last_online` refresh between connect / disconnect lifecycle events. Volume is one per charger per heartbeat interval (~30s default). Large fleets (>1k chargers) should disable this and rely on cp.status / reconnect-driven cp.online instead. | Disabling means `last_online` only advances on connect, status change, transaction events, or boot — an idle but-connected charger will look stale on the backend. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_CP_METER` | `true` | bool | tunable | no | Enable webhook delivery for `cp.meter` MeterValues events. **On by default** so a fresh install gets per-frame consumption rows into the backend's `charging_consumptions` table without extra wiring. Large operators should set this to `false` and consume the `cp.meter` Kafka topic directly — see impact. | MeterValues are high-volume — ~333 webhooks/sec at 10k chargers will saturate the dispatcher's HTTP pool. For fleets >100 chargers, prefer the Kafka consumer path. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_TX_STARTED` | `true` | bool | tunable | no | Enable webhook delivery for `tx.started` events. | Disable to silence transaction-start pushes. |
| `EVEYS_OCPP_WEBHOOK_ENABLE_TX_STOPPED` | `true` | bool | tunable | no | Enable webhook delivery for `tx.stopped` events. Belt-and-braces signal alongside the synchronous `/sessions/close` REST call — see `docs/integration/03-webhooks.md`. | Disable to silence transaction-stop pushes; the synchronous `/sessions/close` is still made by the handler regardless. |
| `EVEYS_OCPP_WEBHOOK_CONSUMER_GROUP` | `eveys-ocpp-webhook-dispatcher` | string | structural | no | Kafka consumer group the webhook dispatcher uses to tail the four event topics. Distinct from the ClickHouse ingestor's group so the two pipelines run independently. | Changing this resets webhook consumer offsets. Keep stable unless intentionally replaying from earliest. |
| `EVEYS_OCPP_WEBHOOK_REQUEST_TIMEOUT_SECONDS` | `10.0` | 1.0–120.0 | tunable | no | HTTP timeout for a single webhook delivery attempt. Backend must respond within this window or the gateway treats the attempt as a transient failure and retries. | Lower = faster failure detection; higher = tolerates slower backends. 10 s matches the backend's documented response budget per `docs/integration/03-webhooks.md`. |
| `EVEYS_OCPP_WEBHOOK_MAX_ATTEMPTS` | `5` | 1–20 | tunable | no | Total delivery attempts before the gateway gives up and logs `webhook.delivery_failed`. Includes the first attempt. | Retries follow exponential backoff: 1 s, 5 s, 30 s, 2 min, 10 min for the default of 5. Lowering reduces the longest in-flight tail; raising tolerates longer backend outages. |

## Prometheus metrics (Phase 4 / E4-1)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_METRICS_ENABLED` | `true` | bool | structural | no | Master switch for the Prometheus metrics server. True in production and local dev. Unit tests flip this to False via an autouse fixture so a `pytest -k something` invocation doesn't try to bind 9100 once per test process. | False removes the /metrics endpoint entirely. Counters/histograms still increment in-process — they just become unscrapeable, so no operational signal. |
| `EVEYS_OCPP_METRICS_HOST` | `0.0.0.0` | string | structural | no | Bind address for the Prometheus scrape server. Default `0.0.0.0` so a sidecar Prometheus scraper inside the same k8s namespace can reach it. | Restrict to `127.0.0.1` if scraping happens inside the same pod (sidecar pattern). In production we expose to the cluster network; the port is not in the public ingress. |
| `EVEYS_OCPP_METRICS_PORT` | `9100` | 1–65535 | structural | no | Scrape port. 9100 by convention (canonically `node_exporter`'s port — we own the gateway port in our deployment). Compose publishes this and Prometheus' ServiceMonitor in k8s targets the same. | Changing this requires updating compose `ports:`, the Helm chart's Service, and the ServiceMonitor selector. The default is fine in 99% of environments. |
| `EVEYS_OCPP_METRICS_PATH` | `/metrics` | string | tunable | no | Path the scrape endpoint serves on. Lets ops mount it at `/internal/metrics` behind a path-based proxy without a code change. Path is matched exactly; trailing slashes are not normalised. | Cosmetic + access-control via reverse-proxy rules. |
| `EVEYS_OCPP_METRICS_INCLUDE_PYTHON_COLLECTORS` | `true` | bool | tunable | no | Whether prometheus_client's default GC / process / platform collectors stay registered. They emit ~12 series an operator rarely needs (`python_gc_objects_collected_total`, `process_resident_memory_bytes`, etc.). Set False to trim them in resource-tight environments. Most fleets keep True — they're free and they catch GC stalls. | False removes `python_*` and `process_*` series from the scrape output. No instrumentation we own depends on them; Grafana dashboards built off our `eveys_ocpp_*` namespace are unaffected. |

## OpenTelemetry tracing (Phase 4 / E4-3)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_TRACING_ENABLED` | `false` | bool | structural | no | Master switch for OpenTelemetry tracing. Default False — tracing is opt-in because most environments don't have an OTLP collector listening, and a tracer with a misconfigured exporter quietly buffers spans until OOM. Flip to True only when `tracing_otlp_endpoint` points at a real collector. | False keeps the global `NoOpTracerProvider` — every `tracer.start_as_current_span(...)` is a few-ns no-op. True activates the SDK; spans flow to the configured OTLP endpoint. |
| `EVEYS_OCPP_TRACING_OTLP_ENDPOINT` | `http://localhost:4317` | string | structural | no | OTLP/gRPC endpoint for span export. Standard collector port is 4317 (gRPC) and 4318 (HTTP); we use gRPC. Honoured only when `tracing_enabled=True`. The dotted-default makes it obvious this isn't yet pointing at a real collector. | Pointing at an unreachable endpoint silently buffers spans (the OTLP exporter retries with backoff). The SDK logs export failures at WARNING — watch for `Failed to export batch` in stderr at boot. |
| `EVEYS_OCPP_TRACING_SAMPLE_RATE` | `1.0` | 0.0–1.0 | tunable | no | Head-based sample rate in `[0.0, 1.0]`. 1.0 traces every request — fine in dev. In production set this to 0.01-0.1 unless your collector is sized for full-rate. | Lowering this drops spans uniformly across all operations. Errors are *not* preferentially kept (no tail-based sampling at the SDK layer); use a collector-side tail sampler if you need that. |
| `EVEYS_OCPP_TRACING_SERVICE_NAME` | `eveys-ocpp` | string | structural | no | `service.name` resource attribute attached to every span. Identifies this service in the trace UI; default matches the python package name. Multiple replicas of the same service share this — `service.instance.id` (auto-set from `pod_id`) discriminates between replicas. | Changing this re-bins all spans under a new service in your trace backend; existing saved searches break. Treat as fixed once a deployment is live. |

## Sentry error tracking (Phase 4 / E4-4)

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_SENTRY_DSN` | (empty) | — | structural | **yes** | Sentry DSN for error tracking. Empty disables the SDK entirely (no init, no transport, no monkey-patches) so the gateway behaves identically to a Sentry-free build. Set in production to capture unhandled exceptions and structured `error`-level logs. Stored as `SecretStr` (E5-7) — the public key embedded in the DSN is enough to ingest events on a project's behalf, so treat it as secret regardless of Sentry's own threat model. | Empty → Sentry is a hard no-op. Non-empty → SDK boots at startup; any later DSN typo surfaces as a `Bad DSN` log line on stderr (the SDK refuses to init silently). |
| `EVEYS_OCPP_SENTRY_ENVIRONMENT` | `development` | string | structural | no | `environment` tag attached to every Sentry event. Conventionally `production`, `staging`, `development`. Sentry alert rules and saved searches typically pivot on this — keep it stable per deployment. | Changing splits the Sentry issue stream — the same exception on the same release line appears as two issues if `environment` differs. |
| `EVEYS_OCPP_SENTRY_RELEASE` | (empty) | string | tunable | no | `release` tag attached to every Sentry event. Empty lets the gateway default to the package `__version__` at boot. Override only when CI injects a richer label (commit SHA, deploy id). | Drives Sentry's regression detection — an issue marked resolved in release X reopens automatically when release Y emits the same fingerprint. |
| `EVEYS_OCPP_SENTRY_TRACES_SAMPLE_RATE` | `0.0` | 0.0–1.0 | tunable | no | Sentry performance / tracing sample rate. Default 0.0 — OTel (E4-3) owns tracing; Sentry's job here is errors only. Setting > 0 doubles the tracing instrumentation cost and fragments traces across two backends; only flip if you specifically want Sentry's `Performance` view. | Above 0 → Sentry SDK monkey-patches httpx / fastapi / asyncio to record spans. May overlap with OTel's instrumentation depending on import order. |
| `EVEYS_OCPP_SENTRY_PROFILES_SAMPLE_RATE` | `0.0` | 0.0–1.0 | tunable | no | Sentry profiling sample rate. Default 0.0 (off). Profiling samples Python frames at ~100 Hz per traced transaction; ignored unless `sentry_traces_sample_rate > 0` since profiling attaches to traces. | Above 0 → SIGPROF-driven sampler runs; ~3-5% steady-state CPU overhead on traced requests. Only useful when chasing per-frame latency. |

## Graceful shutdown

| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |
|---|---|---|---|---|---|---|
| `EVEYS_OCPP_SHUTDOWN_DRAIN_ENABLED` | `true` | bool | tunable | no | When True, SIGTERM/SIGINT trigger a drain phase before the TaskGroup is cancelled: `/api/v1/ready` flips to 503, the load balancer's readiness probe fails, and new WS upgrades stop being routed here. When False, signals cancel the TaskGroup immediately (legacy behaviour). | Disable only as an emergency kill-switch. Without drain, rolling deploys cause brief connection-refused windows until the LB notices the pod is gone. |
| `EVEYS_OCPP_SHUTDOWN_READINESS_PROPAGATION_SECONDS` | `10.0` | 0.0–120.0 | tunable | no | Wall time the gateway holds between flipping `/ready` to 503 and beginning real teardown. Must be >= the load balancer's readiness probe interval x failure threshold so the LB has time to remove this pod from rotation before connections actually drop. | Too low → LB still sends new connections to a draining pod (chargers see refusals). Too high → slow rolling deploys. 10 s suits a 3 s/2-failure k8s probe. |
| `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` | `25.0` | 1.0–300.0 | tunable | no | Hard upper bound on the whole drain → teardown sequence. After this, the TaskGroup is cancelled even if drain hasn't fully completed. Set the k8s `terminationGracePeriodSeconds` to this value plus a small buffer (e.g. +5 s) so kubelet's SIGKILL doesn't beat the gateway's own clean exit. | Bounds worst-case shutdown latency. Must exceed `shutdown_readiness_propagation_seconds` with margin for TaskGroup teardown (bus stop, redis aclose, span flush). |

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
