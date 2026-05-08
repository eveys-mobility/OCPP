# 01 — Roadmap

> Phased plan from foundations to full production rollout. **AI-accelerated**: target ~14 weeks to GA with a 4-person team. Add ~12 weeks for OCPP 2.0.1 + OCTT certification (parallel track).

## Phase overview

| Phase | Name | Duration | Exit criterion |
|---|---|---|---|
| **0** | Foundations | 1 week | Docs approved, repo scaffolded, CI green on empty package |
| **1** | Protocol skeleton | 1 week | `ocpp-gw` accepts a real charger connection, handles 5 core actions, persists to Postgres |
| **2** | Full OCPP 1.6 Core | 2 weeks | All ~25 1.6 Core actions + gRPC API + Kafka events; integration tests passing |
| **3** | Platform integration | 2 weeks | Backend REST integration: gateway calls backend `/authorize` + `/sessions/*`; gateway exposes `/api/v1/...` for read + commands; webhooks deliver async events. Specced in `docs/integration/` (ADR-0023). |
| **4** | Observability + load test | 1 week | Dashboards live, 10k-charger swarm test passes |
| **5** | Hardening | 2 weeks | Graceful drain (rolling-deploy-safe shutdown), rate limits, reconnect-storm test, security review, pen test (core idempotency for `BootNotification`/`StopTransaction` already shipped in Phase 2 via E2-11; remaining handler-level idempotency hardening lands here) |
| **6** | Staging soak | 1 week | 10 dev-fleet chargers running on `ocpp-gw` for 7 days, zero incidents |
| **7** | Production rollout | 4 weeks | Wave ramp 10 → 100 → 1k → 10k → all chargers live |
| **Parallel** | OCPP 2.0.1 + OCTT | 12 weeks (start week 4) | OCTT certification passes; 2.0.1 chargers supported |

**Total to full rollout**: ~14 weeks. **Total including 2.0.1 GA**: ~6 months.

---

## Phase 0 — Foundations (week 0)

**Goal**: alignment, scaffolding, working CI.

| Deliverable | Owner | Done when |
|---|---|---|
| Docs reviewed and approved (this set) | Tech lead + manager | All docs merged on `main` |
| Repo scaffolded: `pyproject.toml`, `Makefile`, `src/`, `tests/`, `proto/` | Senior backend | `make install && make tests` is green on empty package |
| CI on GitHub Actions (Python 3.13) | Senior backend | First PR runs lint + tests automatically |
| Container image builds | Platform / SRE | `docker build` succeeds; image is < 200 MB |
| Implementation plan written | Tech lead | Plan doc merged after docs are approved |

**Exit**: a clone-able repo where `make tests` works and CI is green.

---

## Phase 1 — Protocol skeleton (week 1)

**Status**: ✅ complete. Detail in [`02-tasks.md`](./02-tasks.md#phase-1--protocol-skeleton).

**Goal**: prove the protocol layer works end-to-end against a real charger or simulator.

| Deliverable | Done when |
|---|---|
| `eveys-ocpp` package structure created | `from eveys_ocpp import app` resolves |
| `websockets`-based WS server accepts connections, validates `Sec-WebSocket-Protocol` | Local manual test passes |
| `ocpp.v16.ChargePoint` subclass with **5 handlers**: `BootNotification`, `Heartbeat`, `StatusNotification`, `Authorize`, `StartTransaction` | Charger simulator completes a transaction |
| Postgres schema (`charge_points`, `transactions`) + Alembic migration | `alembic upgrade head` succeeds |
| Settings module (`pydantic-settings`, env-driven) | All config from env vars only |
| Structured logging (`structlog`, JSON output) | Every log line includes `cp_id` |

**Exit**: open a WS to localhost, complete a `BootNotification` → `StartTransaction` → `StopTransaction` round-trip, see rows in Postgres.

---

## Phase 2 — Full OCPP 1.6 Core (weeks 2–3)

**Status**: ✅ exit gate met 2026-05-04. The remaining ~20 long-tail OCPP 1.6 Core handlers ship alongside Phase 3+ as new handlers slot into the existing pipeline. Detail in [`02-tasks.md`](./02-tasks.md#phase-2--full-ocpp-16-core).

**Goal**: complete protocol coverage + clean internal API.

| Deliverable | Done when |
|---|---|
| All ~25 OCPP 1.6 Core actions handled | Each action has handler + unit test + integration test |
| gRPC server (`ocpp_gw.v1`): `RemoteStart`, `RemoteStop`, `Reset`, `ChangeConfiguration`, `TriggerMessage`, `UnlockConnector`, `GetChargerStatus` | Generated clients work in Python, Go, TypeScript |
| Kafka producer for 5 core topics (`cp.connected`, `cp.boot`, `cp.status`, `cp.meter`, `tx.started`) | Events visible via `kcat` |
| Redis-backed online registry (`cp_id → pod_id`) with TTL refresh | Multi-pod E2E test passes |
| Cross-pod command bus (Redis pub/sub) | gRPC call to wrong pod gets routed correctly |
| Idempotency cache for inbound `BootNotification` / `StopTransaction` | Replay test passes |

**Exit**: a multi-pod local cluster handles all 1.6 actions, fan-out to Kafka works, gRPC commands reach the right pod.

---

## Phase 3 — Platform integration (weeks 4–5)

**Status**: ✅ complete (2026-05-07). Detail in [`02-tasks.md`](./02-tasks.md#phase-3--platform-integration).

**Goal**: wire the gateway to the Eveys backend.

The integration shape is documented in [`docs/integration/`](./integration/README.md) and locked in [ADR-0023](./adr/0023-backend-rest-integration.md): two REST surfaces (gateway calls backend on the OCPP hot path; backend calls gateway for state and commands) plus async webhooks.

| Deliverable | Done when |
|---|---|
| Gateway HTTP client wired to backend `/authorize`, `/sessions/open`, `/sessions/close`, `/charge-points/register` | OCPP `Authorize`/`StartTransaction`/`StopTransaction`/`BootNotification` handlers consult the backend; charger sees real RFID outcomes |
| Redis-cached `Authorize` (TTL 30s) | P99 `Authorize` < 50 ms in load test |
| Gateway-side REST API at `/api/v1/...` — read endpoints + 19 command endpoints | Backend can list chargers, read MeterValues / status history / transactions, and issue any OCPP CSMS command over HTTP |
| Webhook delivery infrastructure (HMAC-signed, retried, dedupable on `event_id`) | `cp.boot` / `cp.status` / `tx.started` / `tx.stopped` deliveries reach the configured URLs |
| Mock backend server published for the gateway team to test against without a live backend | Gateway tests can run against a mock |
| ADR-0023 + `docs/integration/` merged | Backend dev has a frozen contract to implement against |

**Exit**: a charging session driven by an RFID tap reaches the backend's authorize endpoint, opens a session, closes a session, and the backend can read the resulting MeterValues time-series via the gateway's `/api/v1/...` surface.

---

## Phase 4 — Observability + load test (week 6)

**Status**: ✅ complete (2026-05-07). Detail in [`02-tasks.md`](./02-tasks.md#phase-4--observability--load-test).

**Goal**: see what we have; prove it scales.

| Deliverable | Done when |
|---|---|
| Prometheus metrics exposed; Grafana dashboards: fleet overview, per-pod, per-charger, reconnect storms, transaction funnel | All 5 dashboards render with real data |
| OpenTelemetry tracing on every inter-service call | A `RemoteStart` shows as one trace mobile → BFF → gRPC → WS → charger |
| Sentry integrated for error capture | New errors group + alert on first occurrence |
| Charger simulator (CLI) capable of running 10k concurrent virtual chargers | `eveys-ocpp-sim --count 10000` runs |
| Load test: 10k chargers, 1k transactions/min for 1h | P95 `RemoteStart` < 3s; zero lost transactions |
| Reconnect-storm test: kill 50% of pods | Fleet recovers in < 60s, Postgres steady |

**Exit**: dashboards green under 10k-charger load; storm test passes.

---

## Phase 5 — Hardening (weeks 7–8)

**Goal**: production-ready security and resilience.

| Deliverable | Done when |
|---|---|
| Graceful drain on SIGTERM (`/api/v1/ready` flips, LB removes pod from rotation, then teardown) | Rolling restart causes no charger connection refusals |
| Per-IP rate limit on WS upgrade (Envoy) | DoS test from one IP gets throttled |
| Per-charger message rate cap | Flooding charger triggers backpressure, not crash |
| Sanity-range validation on `MeterValues` | Charger reporting 100 MWh tx is quarantined |
| mTLS between internal services (SPIFFE/Linkerd or manual certs) | Pod-to-pod calls require valid cert |
| Basic Auth at WS edge with per-CP password rotation | Auth failures rate-limited; rotated quarterly |
| Secrets in vault (AWS Secrets Manager or HashiCorp Vault) | No secrets in env files / git |
| Dependency scanning in CI (`pip-audit`, `dependabot`) | New CVEs surface within 24h |
| External pen test | Report received; criticals fixed |
| DR drill: kill DB primary, kill Redis, kill 1/3 pods | Service recovers without data loss |

**Exit**: pen test report acceptable; DR drill passes.

---

## Phase 6 — Staging soak (week 9)

**Goal**: prove it works for *real* chargers, in a controlled scope.

| Deliverable | Done when |
|---|---|
| 10 dev-fleet chargers configured against `ocpp-gw` staging endpoint | Chargers connect, heartbeat, transact for 7 days |
| Per-charger health dashboards (error rate, msg/s, reconnects) | Dashboard live during soak |
| Rollback runbook tested — disconnect a charger from `ocpp-gw` in < 2 min | Drill performed and timed |

**Exit**: 10 real chargers run on `ocpp-gw` in staging for 7 consecutive days with zero incidents.

---

## Phase 7 — Production rollout (weeks 10–13)

**Goal**: bring the production fleet onto `ocpp-gw` in safe, gated waves.

| Wave | Chargers | Gate to next | Duration |
|---|---|---|---|
| W1 | 10 | 7 days, error rate < 0.1%, zero data discrepancies | week 10 |
| W2 | 100 | 5 days, same gates | week 10 |
| W3 | 1,000 | 3 days, same gates | week 11 |
| W4 | 10,000 | 3 days, same gates | week 12 |
| W5 | All remaining | full rollout | week 13 |

| Supporting deliverable | Done when |
|---|---|
| Per-`cp_id` allowlist controlling which chargers connect | Allowlist file controls the wave |
| Reconciliation report (transactions / meter values) per wave | Run nightly; zero discrepancies = green |

**Exit**: 100% of fleet on `ocpp-gw`.

---

## Parallel — OCPP 2.0.1 + OCTT (weeks 4–16)

**Goal**: support 2.0.1 chargers and pass OCA certification.

| Deliverable | Target week |
|---|---|
| OCPP 2.0.1 handler scaffolding (using `ocpp.v201`) | week 4 |
| Device Model variables/components | week 6 |
| OCTT in CI (continuous compliance testing) | week 8 |
| First OCTT pass on Core profile | week 10 |
| Full Core + Authorization + LocalAuthList + Smart Charging profiles | week 14 |
| OCTT certification submission | week 16 |

---

## Risks tracked here

Top three:

1. **Charger firmware caches CSMS URL** → push `ChangeConfiguration` to update CSMS URL before each rollout wave.
2. **Vendor interop bugs** → wave-based rollout surfaces them per-vendor early.
3. **OCTT first-time pass is rare** → 2-month tail budgeted in the parallel track.

---

## How to update this roadmap

- Dates slip → update the affected phase, add a one-line "slipped because…" note. Never silently shift.
- New phase needed → add it; renumber only if absolutely necessary.
- Phase exit criterion changes → require an ADR.
