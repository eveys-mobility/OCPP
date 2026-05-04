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
| E1-8 | Handler: `Authorize` | ✅ done (auth-service mocked; real call in E3-3) |
| E1-9 | Handler: `StartTransaction` / `StopTransaction` | ✅ done — `StartTransaction` emits `tx.started` to Kafka (E2-8); `StopTransaction` is replay-gated by Redis idempotency cache (E2-11) with the pre-existing DB-layer natural-key dedup as defense in depth |
| E1-10 | Postgres schema (`charge_points`, `transactions`) + Alembic migration | ✅ done |
| E1-11 | Structured logging on every message in/out (`cp_id`, `message_id`, `action`, `direction`) | ✅ done |
| E1-12 | Local docker-compose for dev (Postgres + Redis + Kafka + ClickHouse + the service) per [`07-local-dev-setup.md`](./07-local-dev-setup.md) | ✅ done — `make compose-up` brings full stack including service container to healthy |
| E1-13 | Smoke test: charger simulator → real round-trip → assertions on Postgres rows | ✅ done — `tests/e2e/test_local_smoke.py::test_full_charger_round_trip` + `test_stop_transaction_replay_is_idempotent` pass against live stack |

---

## Phase 2 — Full OCPP 1.6 Core

| ID | Task | Output |
|---|---|---|
| E2-1 | Implement remaining ~20 OCPP 1.6 Core actions (handlers + tests) | 🟡 in progress — `MeterValues` shipped (handler + sanity-range quarantine + Kafka publish via `EventProducer`; unit tests + e2e); rest of Core actions still TODO |
| E2-2 | Define `proto/ocpp_gw/v1/gateway.proto` — gRPC commands | ✅ done — service `OcppGateway` with 7 RPCs; canonical gRPC status codes + per-RPC outcome enums; SmartCharging types stubbed |
| E2-3 | Define `proto/events/v1/events.proto` — Kafka event envelopes | ✅ done — single `EventEnvelope` with `oneof payload`; 5 topics (`cp.connected` / `cp.boot` / `cp.status` / `cp.meter` / `tx.started`); `cp_id` as Kafka partition key per AGENTS rule |
| E2-4 | gRPC server scaffolding (`grpclib`) | ✅ done — `OcppGatewayService` implements all 7 RPCs as `UNIMPLEMENTED` placeholders (real bodies land E2-5/E2-6); `__main__.py` runs WS + gRPC concurrently via `asyncio.TaskGroup`; `make protoc` regenerates from `proto/`; runtime image stays at 171 MB via two-stage Docker build |
| E2-5 | Implement gRPC `RemoteStart` end-to-end | ✅ done (same-pod) — gRPC call → `ConnectionMap` lookup → OCPP `RemoteStartTransaction.req` → 30s timeout → translated reply. Off-pod returns `UNAVAILABLE` with owning pod_id pending E2-10. Offline returns `NOT_FOUND`. Live e2e verified with a charger sim |
| E2-6 | Implement remaining gRPC commands | ✅ done (same-pod) — `RemoteStop`, `Reset` (Hard/Soft), `ChangeConfiguration` (Accepted / Rejected / RebootRequired / NotSupported), `TriggerMessage` (six message kinds), `UnlockConnector` (Unlocked / UnlockFailed / NotSupported), and read-only `GetChargerStatus` (registry online/pod_id + Postgres `last_status`/`last_heartbeat_at`). Shared `_dispatch_ocpp_call` helper handles cp resolution + 30s timeout uniformly. Boundary validation: empty `cp_id` / empty `key` / `connector_id<=0` / `RESET_TYPE_UNSPECIFIED` / `TRIGGER_MESSAGE_TYPE_UNSPECIFIED` → `INVALID_ARGUMENT`. e2e tests cover RemoteStop dispatch + GetChargerStatus online/offline. Cross-pod routing still pending E2-10. |
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

| ID | Task | Output |
|---|---|---|
| E3-1 | Define gRPC contracts with platform services (`auth`, `session`, `device`) | Frozen `.proto` files with semver |
| E3-2 | Implement gRPC clients in `eveys_ocpp.platform_client` | All 3 clients with timeout + retry + circuit breaker |
| E3-3 | Wire `Authorize` → `auth-service.CheckAuthorization` | Real auth check works |
| E3-4 | Cache `Authorize` results in Redis (TTL 30s) | P99 < 50ms in load test |
| E3-5 | Wire `StartTransaction` → `session-service.OpenSession` | tx_id issued by session-service |
| E3-6 | Wire `StopTransaction` → `session-service.CloseSession` | Session closed in session-service |
| E3-7 | Mobile BFF subscribes to `cp.status` (Kafka) | Mobile dev confirms live status |
| E3-9 | Publish mock `ocpp-gw` server (Python package) | Downstream teams develop without us |
| E3-10 | Document gRPC versioning + deprecation policy | ADR added |

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
