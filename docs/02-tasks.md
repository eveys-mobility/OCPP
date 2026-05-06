# 02 — Tasks

> Concrete work breakdown by phase. Each task has an ID (`E0-1`, `E1-3`, …) so they can be referenced from MRs, issues, and the implementation plan.

**ID format**: `E<phase>-<seq>` (e.g. `E2-7` = Phase 2, task 7). IDs are stable; tasks can be re-ordered freely.

---

## Phase 0 — Foundations

| ID | Task | Output | Estimate |
|---|---|---|---|
| E0-1 | Set up monorepo at `eveys/` | `eveys/ocpp/` exists, git initialized | ✅ done |
| E0-2 | Create Python project skeleton (`pyproject.toml`, `Makefile`, `src/eveys_ocpp/`, `tests/`) | `make install && make tests` green | ✅ done |
| E0-3 | CI pipeline (GitLab CI): lint + test on Python 3.13 | ✅ done — `.gitlab-ci.yml` runs `lint` + `types` + `tests` (with Cobertura coverage report) on every MR + every `main` push; venv cached by pyproject hash |
| E0-4 | Pre-commit config (ruff lint + format, mypy --strict on `src/`, stock hygiene, conventional-commit message check) | ✅ done — `.pre-commit-config.yaml` runs on every commit; `make install` activates hooks; `make precommit` runs against all files |
| E0-5 | Dockerfile (distroless Python 3.13 base, multi-stage) | Image < 200 MB, runs `eveys-ocpp --version` | ✅ done — `eveys-ocpp:dev` is 168 MB, runs `--version` correctly, ships in compose stack |
| E0-6 | `AGENTS.md` and `CLAUDE.md` at repo root | AI assistants pick up rules in any IDE | 0.5d |
| E0-7 | `.editorconfig`, `.gitignore`, `.gitattributes` | Standard project hygiene | ✅ done |
| E0-8 | Implementation plan written and reviewed | `docs/06-implementation-plan.md` merged | 1d |
| E0-9 | `make doctor` target — checks versions of Python, Docker, kubectl, helm, k3d/kind against `docs/07-local-dev-setup.md` and prints what's missing | New engineers run one command and learn what to install | ✅ done |
| E0-10 | Test trust ladder (Tier-3 compose smoke) — production-shaped `docker compose up` exercised by CI | Pre-merge guarantee: a green pipeline implies the deployable stack actually boots and stays up | ✅ done — adds `tests/compose_smoke/` (8 tests against the real built image), `make compose-smoke` target, GitLab `tests:compose-smoke` job (Docker-in-Docker), [ADR-0024](./adr/0024-test-trust-ladder.md), [docs/10-testing-strategy.md](./10-testing-strategy.md). Also fixes three deploy-time bugs the new tier caught: (1) `Dockerfile` `ENTRYPOINT` shape silently swallowed compose `command:` overrides → ingestor container had been running the gateway entrypoint; (2) compose `ocpp` service missing `EVEYS_OCPP_KAFKA_BROKERS` / `REDIS_URL` / `CLICKHOUSE_HOST` so it fell back to `localhost` defaults and crashed at boot; (3) Kafka `KAFKA_ADVERTISED_LISTENERS` only published `localhost:9092`, so in-network clients bootstrapped fine but failed every metadata-driven request — fixed by a dual-listener (HOST://:9092 + INTERNAL://:29092) layout. |
| E0-11 | Spec the configuration-reference work: ADR-0025 + seed `docs/11-configuration-reference.md` + this row, no code | One docs-only MR with the spec; reviewers can sign off on the metadata schema before any field gets backfilled | ✅ done |
| E0-12 | Backfill `Settings` field metadata per ADR-0025: every field gets `description=`, `json_schema_extra={category, impact, secret, stability}`, plus a closed enum (`Literal[...]` or `pattern=`) where the type is currently `str`. ~46 fields. | ✅ done — all fields backfilled; `kafka_acks`, `log_level`, `backend_authorize_fallback` tightened with `Literal[...]`; `db_url` and `backend_token` flagged secret; `tests/unit/test_settings_metadata.py` enforces the schema. |
| E0-13 | Implement `scripts/render_config_reference.py` — walks `Settings.model_fields`, groups by category, emits Markdown identical to today's `docs/11-configuration-reference.md` shape. Also emits `.env.example` (secrets blanked out). Idempotent: same model → byte-identical output. `make config-export` runs it. | ✅ done — generator emits both files; `make config-export` / `make config-export-check` wired; the hand-written seed has been replaced by generator output; `tests/unit/test_render_config_reference.py` is the snapshot gate. ADR-0025's `authorize_cache` category is live: E3-4's `BACKEND_AUTHORIZE_CACHE_ENABLED` and `BACKEND_AUTHORIZE_CACHE_TTL_SECONDS` use it. |
| E0-14 | CI staleness gate — a `config-reference-fresh` job that runs the generator and `git diff --exit-code` against the committed file. Fails MRs that change `Settings` without regenerating. Same shape as the proto-breaking gate (ADR-0018). | ✅ done — `config-reference-fresh` job in `.gitlab-ci.yml` quality stage runs `scripts/render_config_reference.py --check` on every MR + push to main; hard-fails on drift. |
| E0-15 | Per ADR-0025, surface the metadata via a future-friendly export: a `make config-schema` target that prints the JSON Schema (Pydantic's built-in `model_json_schema()`) so a Helm-values-validator or operator UI can consume it without re-parsing Markdown. Plumbing only — no consumer yet. | ✅ done — `make config-schema` prints `Settings.model_json_schema()` to stdout (pipe to a file when needed). All 45 fields with `category`/`impact`/`secret`/`stability` metadata appear in the output. |

---

## Phase 1 — Protocol skeleton

| ID | Task | Output |
|---|---|---|
| E1-1 | Add runtime deps: `ocpp`, `websockets`, `pydantic-settings`, `structlog`, `asyncpg`, `alembic`, `sqlalchemy[asyncio]` | ✅ done |
| E1-2 | Settings module: env-driven config, validated with `pydantic-settings` | ✅ done |
| E1-3 | `eveys_ocpp.transport.ws_server` — `websockets.serve` + subprotocol negotiation + auth hook | ✅ done (auth hook stubbed; lands properly in Phase 5) |
| E1-4 | `eveys_ocpp.connection.ChargePoint` subclass of `ocpp.v16.ChargePoint` | ✅ done |
| E1-5 | Handler: `BootNotification` | ✅ done — replay-gated by the Redis idempotency cache (E2-11), Kafka emit on `cp.boot` (E2-8) |
| E1-6 | Handler: `Heartbeat` | ✅ done — refreshes Redis TTL via E2-9 registry; re-claims ownership if key expired between heartbeats |
| E1-7 | Handler: `StatusNotification` | ✅ done — Kafka emit on `cp.status` (E2-8); per-state history reaches ClickHouse via E2-14 |
| E1-8 | Handler: `Authorize` | ✅ done (replies `Accepted` for any `id_tag` until backend is wired in E3-3) |
| E1-9 | Handler: `StartTransaction` / `StopTransaction` | ✅ done — `StartTransaction` emits `tx.started` to Kafka (E2-8); `StopTransaction` is replay-gated by Redis idempotency cache (E2-11) with the pre-existing DB-layer natural-key dedup as defense in depth |
| E1-10 | Postgres schema (`charge_points`, `transactions`) + Alembic migration | ✅ done |
| E1-11 | Structured logging on every message in/out (`cp_id`, `message_id`, `action`, `direction`) | ✅ done |
| E1-12 | Local docker-compose for dev (Postgres + Redis + Kafka + ClickHouse + the service) per [`07-local-dev-setup.md`](./07-local-dev-setup.md) | ✅ done — `make compose-up` brings full stack including service container to healthy |
| E1-13 | Smoke test: charger simulator → real round-trip → assertions on Postgres rows | ✅ done — `tests/e2e/test_local_smoke.py::test_full_charger_round_trip` + `test_stop_transaction_replay_is_idempotent` pass against live stack |

---

## Phase 2 — Full OCPP 1.6 Core

> **Phase exit gate met 2026-05-04** — E2-2 through E2-14 are ✅; the long-tail E2-1 (remaining ~20 OCPP 1.6 Core handlers) continues alongside Phase 3 because the gateway is already publishing every billing-relevant event to Kafka and ClickHouse. New handlers slot into the existing pipeline without further infrastructure work.

| ID | Task | Output |
|---|---|---|
| E2-1 | Implement remaining ~20 OCPP 1.6 Core actions (handlers + tests) | ✅ done — Long-tail closed. **E2-1A** (Core completion) shipped `DataTransfer` (in + out), `GetConfiguration` (out), `ClearCache` (out) — OCPP 1.6 **Core profile** complete. **E2-1A.e2e** shipped two-pod cross-bus e2e for the three new RPCs and extended the bus envelope to ferry full OCPP response payloads (`BusReply.ocpp_response`). **E2-1B** LocalAuthList shipped: gRPC `GetLocalListVersion` + `SendLocalList`, two new Postgres tables (`local_auth_lists`, `local_auth_list_entries`) via Alembic migration `0002`. **E2-1C** Reservations shipped: gRPC `ReserveNow` + `CancelReservation`, new `reservations` Postgres table via Alembic migration `0003`, gateway-assigned `reservation_id`. Charger-side authority documented in **ADR-0021**. **E2-1F** Diagnostics + Firmware shipped: outbound gRPC `GetDiagnostics` + `UpdateFirmware`, inbound `DiagnosticsStatusNotification` + `FirmwareStatusNotification`. Two new latest-wins columns on `charge_points` via Alembic `0004`. **E2-1E** Smart Charging shipped: gRPC `SetChargingProfile`, `ClearChargingProfile`, `GetCompositeSchedule`; new `charging_profiles` + `charging_schedule_periods` tables via Alembic `0005`; charger-side resolver (ADR-0022) — gateway stores input profiles, GetCompositeSchedule round-trips to the charger. gRPC surface is now 19 RPCs; OCPP 1.6 Core / LocalAuthList / Reservations / FirmwareManagement / Smart Charging / RemoteTrigger profiles all have CSMS-side handlers shipped. Promotion to ✅ on the conformance matrix is blocked on OCTT (task C-1a, deferred). |
| E2-2 | Define `proto/ocpp_gw/v1/gateway.proto` — gRPC commands | ✅ done — service `OcppGateway` with 7 RPCs; canonical gRPC status codes + per-RPC outcome enums; SmartCharging types stubbed |
| E2-3 | Define `proto/events/v1/events.proto` — Kafka event envelopes | ✅ done — single `EventEnvelope` with `oneof payload`; 5 topics (`cp.connected` / `cp.boot` / `cp.status` / `cp.meter` / `tx.started`); `cp_id` as Kafka partition key per AGENTS rule |
| E2-4 | gRPC server scaffolding (`grpclib`) | ✅ done — `OcppGatewayService` implements all 7 RPCs as `UNIMPLEMENTED` placeholders (real bodies land E2-5/E2-6); `__main__.py` runs WS + gRPC concurrently via `asyncio.TaskGroup`; `make protoc` regenerates from `proto/`; runtime image stays at 171 MB via two-stage Docker build |
| E2-5 | Implement gRPC `RemoteStart` end-to-end | ✅ done — gRPC call → `ConnectionMap` lookup → OCPP `RemoteStartTransaction.req` → 30s timeout → translated reply. Off-pod is now routed via the cross-pod command bus (E2-10, see ADR-0016); the `UNAVAILABLE` fallback only fires when no bus is configured (e.g. dev). Offline returns `NOT_FOUND`. Live e2e verified with a charger sim plus the two-pod scenario in `tests/e2e/test_two_pod_dispatch.py`. |
| E2-6 | Implement remaining gRPC commands | ✅ done — `RemoteStop`, `Reset` (Hard/Soft), `ChangeConfiguration` (Accepted / Rejected / RebootRequired / NotSupported), `TriggerMessage` (six message kinds), `UnlockConnector` (Unlocked / UnlockFailed / NotSupported), and read-only `GetChargerStatus` (registry online/pod_id + Postgres `last_status`/`last_heartbeat_at`). Shared `_dispatch_ocpp_call` helper handles cp resolution + 30s timeout uniformly. Boundary validation: empty `cp_id` / empty `key` / `connector_id<=0` / `RESET_TYPE_UNSPECIFIED` / `TRIGGER_MESSAGE_TYPE_UNSPECIFIED` → `INVALID_ARGUMENT`. e2e tests cover RemoteStop dispatch + GetChargerStatus online/offline. Cross-pod routing flows through the command bus (E2-10, see ADR-0016). |
| E2-7 | Kafka producer (`aiokafka`) | ✅ done — `acks=all` (durable to leader crash) + `enable_idempotence=True` (no duplicate events on producer retry) + tightened `request_timeout_ms=30s` and `retry_backoff_ms=200ms`. Compromise `linger_ms=5` shared across all four topics (aiokafka has no per-topic linger; 5ms gives `cp.meter` batching headroom without putting a real latency floor on `tx.started`/`cp.boot`/`cp.status`). All knobs env-tunable via new `Settings` fields. Per-handler try/except guard from E2-8 stays as the second line of defense. See ADR-0019. |
| E2-8 | Wire each handler to publish its event | ✅ done — `MeterValues` → `cp.meter`, `BootNotification` → `cp.boot`, `StatusNotification` → `cp.status`, `StartTransaction` → `tx.started`. Each handler builds an `EventEnvelope` with `cp_id` Kafka partition key and the matching `oneof` payload, then publishes inside a `try/except`: a broker drop logs `<handler>.publish_failed` and is dropped, never raised — chargers retry aggressively, a flaky broker would otherwise DoS the gateway. Postgres writes happen before the publish so only durably-recorded transactions emit `tx.started`. `Heartbeat` and `Authorize` deliberately have no event (internal RPC; would just be noise). Unit tests cover happy path + producer-None + broker-down for all four emitters. |
| E2-9 | Redis online registry: `cp:online:{cp_id} → pod_id` with 120s TTL | ✅ done — `Registry` class wraps redis.asyncio; WS connect → `mark_online`, Heartbeat → `refresh` (re-claims if expired), WS disconnect → compare-and-delete via Lua to avoid clobbering a reconnected charger on another pod; `pod_id` from Settings (defaults to hostname; K8s downward API in prod); e2e test asserts key lifecycle |
| E2-10 | Cross-pod command bus: pub/sub on `cp:cmd:{cp_id}` channel | ✅ done — `CommandBus` in `src/eveys_ocpp/bus.py` wraps Redis pub/sub on `cp:cmd:{cp_id}` (request) and `cp:reply:{request_id}` (reply) with JSON envelopes (`v=1`, `request_id`, `deadline_ms`). Owning side reconstructs OCPP dataclasses via `_OCPP_CALL_DISPATCH` in `grpc_server.py`; same-pod path is unchanged. Pod-level pattern subscriptions (`cp:cmd:*`, `cp:reply:*`) keep subscription count O(1) per pod regardless of charger count. Two-pod e2e (`tests/e2e/test_two_pod_dispatch.py`) covers RemoteStart, RemoteStop, and the disconnect-mid-flight race; unit tests in `tests/unit/test_bus.py` cover envelope handling, version skew, expired deadlines, and dispatcher exceptions. See ADR-0016. |
| E2-11 | Idempotency cache for `BootNotification` and `StopTransaction` | ✅ done — Redis-backed `IdempotencyCache` in `src/eveys_ocpp/idempotency.py` keyed by `(cp_id, message_id)` with TTL 300s. Atomic `SET NX EX` for test-and-set. `EveysChargePoint.on_boot_notification` and `.on_stop_transaction` declare `call_unique_id` so the OCPP library forwards the MessageId; handlers consult the cache before any DB write or Kafka emit, return canonical Accepted on cache hit. Cache outage falls through (log + proceed as miss) so a Redis blip doesn't wedge handlers. `StopTransaction` keeps its DB-layer natural-key dedup as defense in depth. See ADR-0017. Replay tests cover happy path + cache outage + missing message_id + missing cache instance for both handlers. |
| E2-12 | gRPC contract: backward-compat tests (no breaking changes by accident) | ✅ done — `buf breaking` runs on every MR pipeline against `main` baseline; `WIRE_JSON` rule set covers binary-protobuf and protobuf-JSON breakage; both `proto/ocpp_gw/v1/` and `proto/events/v1/` in scope. Hard-fail by default (the protos are explicitly frozen v1 — `proto/README.md`). Intentional v1→v2 breaks bypass via the `bypass-breaking-change` MR label, which per ADR-0018 also requires tech-lead approval and a same-MR package version bump. New CI job `proto-breaking` in the `quality` stage; `buf` binary version pinned. See ADR-0018. |
| E2-13 | ClickHouse table schemas for `MeterValues`, `StatusNotifications`, `BootNotifications`, `StartTransactions` (DDL + Alembic-equivalent migration tool) | ✅ done — four `MergeTree` tables (`cp_meter`, `cp_status`, `cp_boot`, `tx_started`) in `src/eveys_ocpp/clickhouse/ddl/`, partitioned `toYYYYMM(occurred_at)` and ordered `(cp_id, occurred_at)` per ADR-0020. `cp_meter.sampled_values` uses ClickHouse's native `Nested` type. Plain SQL files + a 60-line `migrate.py` loader keyed on a `schema_migrations` tracking table; idempotent. `make ch-migrate` runs it against either compose or a fresh local install (auto-creates the database). Heartbeats are intentionally absent — they're absorbed by the Redis online registry (E2-9), not a Kafka topic. See ADR-0020. |
| E2-14 | Kafka → ClickHouse ingestion (Kafka table engine, or sidecar consumer if engine constraints push us off it) | ✅ done — sidecar consumer in `src/eveys_ocpp/clickhouse/ingestor.py` (`python -m eveys_ocpp.clickhouse.ingestor`). Subscribes to all four event topics with `aiokafka`, parses the `EventEnvelope`, dispatches by `oneof payload` to the matching ClickHouse table via `asynch`. Batches 500 rows / 5s (env-tunable). At-least-once semantics: offsets commit only after successful INSERT; `event_id` is the downstream dedup key. Sidecar runs as its own compose service `clickhouse-ingestor`. Two e2e tests (`test_clickhouse_schema.py`, `test_kafka_to_clickhouse.py`) verify schema landing and end-to-end Kafka → CH round-trip. ADR-0020 records the sidecar-over-Kafka-Engine choice and the conventions every future event-table inherits. |

---

## Phase 3 — Platform integration

Integration shape locked in [ADR-0023](./adr/0023-backend-rest-integration.md) and specced in [`docs/integration/`](./integration/README.md): two REST surfaces (gateway calls backend on the hot path; backend calls gateway for state + commands) plus async webhooks. JSON over HTTP with bearer-token auth; mTLS overlays in Phase 5.

| ID | Task | Output |
|---|---|---|
| E3-1 | Backend REST contract finalised: `docs/integration/01-backend-rest-contract.md` + ADR-0023 reviewed and merged | Contract frozen for the backend dev to implement against |
| E3-2 | Implement `eveys_ocpp.platform.client` — async HTTP client (`httpx`) with bearer auth, timeout, retry, circuit breaker, structured logging | ✅ done — `src/eveys_ocpp/platform/` ships `BackendHTTPClient` + 5 typed result dataclasses (`AuthorizeResult`, `SessionOpenResult`, `SessionCloseResult`, `ChargePointRegisterResult`, `IdTagInfo`) + 6 typed exceptions (`BackendAuthError`, `BackendBusinessError`, `BackendCircuitOpenError`, `BackendNetworkError`, `BackendTimeoutError`, `BackendUnavailableError`) + an in-house `CircuitBreaker` (closed → open → half-open → closed). 4xx is non-retryable (no breaker hit); 5xx + network + timeout retry per-endpoint and count toward the breaker. Per-endpoint timeouts and retry budgets are env-tunable via Settings. `httpx>=0.28` promoted to runtime dep. 20 unit tests cover happy path against the mock backend, business / auth errors, retry behaviour, breaker integration, header pass-through. The OCPP handlers wire to this in E3-3..E3-6. |
| E3-3 | Wire `Authorize` handler → `POST /api/eveys/authorize` | ✅ done — `EveysChargePoint` gains an optional `backend_client`; `__main__.py` constructs it from settings when `backend_base_url` is non-empty (otherwise None for W1 dev / unit tests). Handler delegates to `BackendHTTPClient.authorize` with `Idempotency-Key=ocpp-auth-{cp_id}-{id_tag}-{message_id}` and forwards the typed `IdTagInfo.status` (Accepted / Blocked / Expired / Invalid / ConcurrentTx) verbatim to the charger. Unknown / forward-compat statuses map to Invalid. `BackendBusinessError` (e.g. UNKNOWN_ID_TAG) → Invalid. `BackendUnavailableError` (timeout / network / circuit-open) honours `backend_authorize_fallback`: `reject` → Invalid (default; safe), `accept_offline` → Accepted with a 5-min expiry (operator opt-in). 12 unit tests cover all paths. |
| E3-4 | Cache `Authorize` results in Redis (TTL 30s) | ✅ done — `src/eveys_ocpp/platform/cache.py` ships `AuthorizeCache` keyed `auth:{cp_id}:{id_tag}` with JSON-serialised `IdTagInfo` and a configurable TTL (default 30 s, env-tunable via `BACKEND_AUTHORIZE_CACHE_TTL_SECONDS`). Cache lookup runs before the backend round-trip; hit → return immediately, miss → backend call, success → cache. Defensive: malformed values, parse errors, and Redis blips on read all fall through to the backend; Redis failures on write are swallowed. `BackendBusinessError` and `BackendUnavailableError` outcomes are deliberately NOT cached (a backend fix should reach the charger on the next tap). Disable knob: `BACKEND_AUTHORIZE_CACHE_ENABLED=false` short-circuits Redis entirely. Cache constructed in `__main__.py` only when `backend_client` is wired and the toggle is on. 18 unit tests (12 cache + 6 handler integration). |
| E3-5 | Wire `StartTransaction` handler → `POST /api/eveys/sessions/open`; wire `BootNotification` handler → `POST /api/eveys/charge-points/register` | ✅ done — Both handlers call the typed `BackendHTTPClient` methods (already shipped in E3-2). Boot inserts the call between the local DB upsert and the Kafka emit; the backend's `registration_status` and `heartbeat_interval_seconds` flow through verbatim (Accepted / Pending / Rejected; unknown → Pending per OCPP forward-compat). Idempotency-Key `ocpp-boot-{cp_id}-{message_id}`. New setting `backend_register_fallback` (default `accept_offline`) controls the response on `BackendUnavailableError`; `BackendBusinessError` always returns Rejected. `cp.boot` only emits on Accepted (downstream wouldn't materialize a Pending/Rejected charger). StartTransaction inserts the call between the DB row insert and the Kafka emit; assigned `transaction_id` becomes the Idempotency-Key (`ocpp-session-open-{transaction_id}`). Backend's `id_tag_info.status` flows through; backend unavailable = keep row, reply Accepted (the row is the audit-of-record, reconciler heals); business rejection = keep row, reply Invalid. The replay-cache hit on Boot short-circuits before the backend call (preserves E2-11 semantics). 12 new unit tests across the two handlers. |
| E3-6 | Wire `StopTransaction` handler → `POST /api/eveys/sessions/close` | ✅ done — Handler calls `BackendHTTPClient.close_session` only on a real first-time stop (DB `applied=True` AND no E2-11 cache hit). Idempotency-Key `ocpp-session-close-{transaction_id}-{message_id or 'no-msg-id'}` per the contract. Backend's `id_tag_info.status` flows through to the OCPP response via the same status map as Authorize / StartTransaction; an unrecognised status maps to `Invalid` (forward-compat). On `BackendUnavailableError` the row stays stopped and the charger sees `Accepted` — the reconciler heals the backend later. On `BackendBusinessError` the row stays stopped and the charger sees `Invalid` (operations need the trace; the OCPP-level `Invalid` tells the charger that this id_tag is no longer good for the next session). OCPP allows omitting `id_tag` (e.g. `EVDisconnected`); the contract requires a string, so empty `""` flows to the backend. **Replays are guarded twice**: the E2-11 cache hit AND the DB-layer `applied=False` both short-circuit before the backend call — chargers retry aggressively, never double-bill. 14 unit tests (7 existing + 7 new). |
| E3-7 | Implement gateway-side REST API at `/api/v1/...` — read endpoints (charge-points, meter-values, status-history, transactions, reservations, charging-profiles) per `docs/integration/02-gateway-rest-api.md` | 🟡 in progress — **foundation slice merged**: ADR-0026 (framework, auth, pagination, in-process ASGI); FastAPI + uvicorn promoted to runtime deps; new `transport/rest_server.py` runs in the existing `__main__` TaskGroup beside WS + gRPC; new `src/eveys_ocpp/api/` package (auth middleware, error envelope, opaque keyset cursors, request-id correlation); `GET /api/v1/health` (Postgres + Redis component probes); `GET /api/v1/charge-points` (cursor-paginated list w/ vendor + online filters); `GET /api/v1/charge-points/{cp_id}` (detail w/ active reservations + profiles inlined); `GET /api/v1/charge-points/{cp_id}/transactions` (cursor + id_tag/open/from/to filters); `GET /api/v1/transactions/{transaction_id}`. 7 new Settings (`rest_enabled`, `rest_host`, `rest_port`, `rest_inbound_tokens` (secret), `rest_auth_disabled`, `rest_default_page_size`, `rest_max_page_size`); 4 new repository functions; 33 new unit tests covering auth, pagination, health, charge-points, and transactions. **Follow-up commits/MRs**: reservations + charging-profiles list endpoints (Postgres-backed, small); `meter-values` + `status-history` endpoints (need a new ClickHouse async read client). |
| E3-8 | Implement gateway-side command endpoints — 19 HTTP wrappers around the existing gRPC RPCs | ✅ done — 18 POST + 1 GET routes under `/api/v1/charge-points/{cp_id}/commands/` (remote-start, remote-stop, reset, change-/get-configuration, clear-cache, trigger-message, unlock-connector, data-transfer, get-local-list-version, send-local-list, reserve-now, cancel-reservation, get-diagnostics, update-firmware, set-/clear-charging-profile, get-composite-schedule, get-charger-status). All dispatch through the same `OcppGatewayService._dispatch_ocpp_call` core (charger lookup, cross-pod bus routing, 30 s timeout) shared with the gRPC surface. New `api/_commands.py` translates `GRPCError` → `ApiError` per the contract: `NOT_FOUND` → 404 `UNKNOWN_CP_ID`, `UNAVAILABLE` → 503 `CHARGER_OFFLINE`, `DEADLINE_EXCEEDED` → 504 `CHARGER_TIMEOUT`. Mirror writes for SendLocalList / ReserveNow / CancelReservation / SetChargingProfile / ClearChargingProfile preserved per ADR-0021/0022 (charger-first, persistence-best-effort). 43 new unit tests covering all 19 routes, error mapping, and side-effect ordering. `OcppGatewayService.serve_forever` refactored to take a pre-built service instance so REST + gRPC share one. |
| E3-9 | Implement webhook delivery — HMAC-signed, retried, dedupable; per-event URL / enable toggles per `docs/integration/03-webhooks.md` | `cp.boot` / `cp.status` / `cp.firmware_status_changed` / `cp.diagnostics_status_changed` / `tx.started` / `tx.stopped` delivered reliably |
| E3-10 | Publish a mock backend (FastAPI, in-repo) the gateway tests against | ✅ done — `tests/mock_backend/` implements the five backend-side endpoints from `docs/integration/01-backend-rest-contract.md` with simulated responses, bearer-token auth, and an in-memory idempotency cache. Runnable two ways: in-process via `httpx.ASGITransport` (test fixtures) or as a standalone uvicorn process via `make mock-backend` / `python -m tests.mock_backend`. Behaviour controls (`MOCK_BACKEND_BLOCKED_ID_TAGS`, `MOCK_BACKEND_FAIL_AUTHORIZE`, etc.) exercise the gateway's circuit-breaker / fallback paths in E3-2..E3-6. 14 unit tests in `tests/unit/mock_backend/`. |

---

## Phase 4 — Observability + load test

| ID | Task | Output |
|---|---|---|
| E4-1 | Prometheus metrics: connections, messages/s per action, gRPC latency, Postgres timing | `/metrics` exposes ~50 stats |
| E4-2 | Grafana dashboards: fleet overview, per-pod, per-charger, reconnect storms, transactions | All 5 dashboards live |
| E4-3 | OpenTelemetry tracing on every gRPC call and every WS message | One trace per `RemoteStart` end-to-end |
| E4-4 | Sentry integration | Errors group, deploy tags, breadcrumbs |
| E4-5 | Charger simulator CLI (`eveys-ocpp-sim`) — N concurrent virtual chargers | Documented; runs on a laptop for 1k chargers |
| E4-6 | Load test rig (k6 or custom): 10k chargers, 1k tx/min, 1h | Pass criteria documented |
| E4-7 | Reconnect-storm test scenario | Kill 50% of pods; recovery < 60s |
| E4-8 | SLO definitions + dashboards | All 5 SLOs in [Slide 16] tracked |

---

## Phase 5 — Hardening

| ID | Task | Output |
|---|---|---|
| E5-1 | Envoy edge config (TLS, sticky hash, rate limit, slow-start) | Helm chart includes Envoy |
| E5-2 | Per-IP WS-upgrade rate limit | DoS test from 1 IP throttled |
| E5-3 | Per-charger message rate cap (token bucket) | Spam test triggers backpressure |
| E5-4 | Sanity range validation on `MeterValues` | Out-of-range payloads logged + dropped |
| E5-5 | mTLS between internal services | Pod-to-pod requires valid cert |
| E5-6 | Basic Auth at WS edge + per-CP password store | Bad creds rejected at edge |
| E5-7 | Secrets in vault, mounted as env vars | No secrets in repo |
| E5-8 | `pip-audit` + Dependabot in CI | New CVEs surface within 24h |
| E5-9 | External pen test | Report received |
| E5-10 | DR drill (kill DB / Redis / pods) | Recovery < SLO targets |

---

## Phase 6 — Staging soak

| ID | Task | Output |
|---|---|---|
| E6-1 | Provision staging cluster (matches prod topology, smaller scale) | k8s namespace running |
| E6-2 | Configure 10 dev-fleet chargers to staging `ocpp-gw` URL | Chargers connect via `ChangeConfiguration` |
| E6-3 | Per-charger health dashboards | Dashboard live during soak |
| E6-4 | Rollback runbook + drill | Disconnect a charger from `ocpp-gw` in < 2 min |

---

## Phase 7 — Production rollout

| ID | Task | Output |
|---|---|---|
| E7-1 | Per-`cp_id` allowlist controlling which chargers connect | Single config flag admits a charger |
| E7-3 | Nightly reconciliation report (transactions, meter values) | Zero discrepancies = green |
| E7-4 | Wave 1: 10 chargers (week 10) | 7-day soak passes |
| E7-5 | Wave 2: 100 chargers | 5-day soak passes |
| E7-6 | Wave 3: 1,000 chargers | 3-day soak passes |
| E7-7 | Wave 4: 10,000 chargers | 3-day soak passes |
| E7-8 | Wave 5: full rollout | All chargers on `ocpp-gw` |

---

## OCPP conformance — discipline (cross-cutting, applies to 1.6 today and 2.0.1 later)

Conformance work is its own track. Every handler MR must update the matrix; every handler row must be 🟡 until the promotion criteria are met. See [`08-ocpp-conformance.md`](./08-ocpp-conformance.md).

| ID | Task | Output |
|---|---|---|
| C-1 | Obtain OCA OCPP 1.6 spec PDF (Edition 2, 2017-09-28) + Errata sheet v4.0 + Security whitepaper Edition 3; link from shared drive | TL + handler authors + QA have access |
| C-1a | **Begin OCA membership process** (prerequisite for OCTT access) | Manager / TL action; OCA membership active |
| C-1b | Identify OCA-designated test laboratory; scope, price, and pre-book lab slot for W8/W9 | Lab booked; quote on file |
| C-2 | Stand up OCTT locally; first OCTT test case (TC_001 Cold Boot Charge Point) green against `make compose-up` + running CSMS | First OCTT run logged; result recorded in `08-ocpp-conformance.md` |
| C-3 | Wire OCTT 1.6 **Core subset** into CI (non-blocking initially; blocking before W6) | `.gitlab-ci.yml` job `ocpp:octt:1.6:core` runs on every MR |
| C-3a | Wire OCTT **Smart Charging** subset into CI | Job `ocpp:octt:1.6:smart-charging` runs |
| C-3b | Wire OCTT **Reservations + LocalAuthList + RemoteTrigger** subsets into CI | Per-profile CI jobs runs |
| C-3c | Wire OCTT **Advanced Security** subset into CI | Job runs after E5-5 mTLS work |
| C-4 | Promote first row (TC_001 Cold Boot Charge Point) 🟡 → ✅ in `08-ocpp-conformance.md` | Promotion criteria met; OCTT artifact attached to MR |
| C-5 | Promote remaining W1 handler rows → ✅ | Matrix shows all ✅ for handlers shipped in W1 |
| C-6 | Draft three PICS documents (functional, security, performance) per Appendix A of the OCA Certification Procedure | PICS drafts in repo `docs/pics/`; reviewed by TL |
| C-7 | Freeze PICS for cert run | PICS signed off by TL; W8 milestone |
| C-8 | Run CSMS performance measurements per Appendix A.2: Authorize response time, OCPP response timeout | Values recorded in `09-certification-readiness.md`; feed into Phase 4 SLO dashboards |
| C-9 | Lab cert run (P-7 in cert track below) | OCTT report green; cert awarded by OCA |

**Sequencing:**

- **C-1, C-1a, C-1b** start in W0 (project-management work, parallel with engineering). C-1a is the *critical-path dependency* — without OCA membership, OCTT cannot be obtained, and C-2..C-9 are all blocked.
- **C-2** starts as soon as C-1a lands AND the W1 stack is up.
- **C-3** in W2; C-3a/b/c follow as the corresponding profiles' handlers ship.
- **C-4, C-5** during W2 — **before** the bulk of E2-1 (remaining ~20 actions) lands, so each new handler is born with promotion criteria ready.
- **C-6** drafted in W6 alongside the load test (which produces the performance values). **C-7 freeze** at W8.
- **C-8** during W6 load test.
- **C-9** at end of W8 / start of W9.

See [`09-certification-readiness.md`](./09-certification-readiness.md) for the full readiness checklist.

## Parallel — OCPP 2.0.1

| ID | Task | Output |
|---|---|---|
| P-1 | `ocpp.v201` handler scaffolding | All v2.0.1 actions stubbed |
| P-2 | Device Model variables / components / monitoring | Spec'd `proto` representation |
| P-3 | Implement Core profile end-to-end | OCTT Core profile passes |
| P-4 | Implement Authorization + LocalAuthList profiles | OCTT relevant profiles pass |
| P-5 | Implement Smart Charging profile | OCTT passes |
| P-6 | Extend OCTT in CI to cover 2.0.1 (subsumes the 1.6-only C-3 with a wider matrix) | Every MR runs OCTT subset across both versions |
| P-7 | OCTT certification submission to OCA | Certification received |

---

## Cross-cutting tasks

| ID | Task | Output |
|---|---|---|
| X-1 | Test coverage gates: 80% on `src/eveys_ocpp/` | CI fails below threshold |
| X-2 | Documentation kept current alongside code | MRs touching `src/` must update relevant docs |
| X-3 | Quarterly dependency upgrade sprint | All deps on latest minor |
| X-4 | Quarterly chaos drill | Documented runbook outcome |
| X-5 | Per-incident post-mortem within 48h | Stored in `docs/postmortems/` |

---

## Conventions for tasks

- Each task ID is **stable** once assigned. Don't reuse IDs even if a task is dropped.
- A task is "done" when its output line is satisfied **and** there's a merged MR + green CI + updated docs.
- Tasks that can be parallelized are tagged `[P]` in the implementation plan.
- Tasks that block others get explicit dependency arrows in the plan, not in this list (this list stays declarative).
