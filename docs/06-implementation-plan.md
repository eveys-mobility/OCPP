# 06 — Implementation Plan

> Week-by-week schedule mapping every task from [`02-tasks.md`](./02-tasks.md) to calendar time. Targets **~14 weeks to full production rollout** with a **4-person team**, AI-accelerated. OCPP 2.0.1 + OCTT certification runs as a **parallel track** from week 4 (~12 additional weeks).

## How to read this plan

- **Calendar weeks numbered W0 (Phase 0) through W13 (Phase 7).**
- Each week lists **deliverables** (what ships), **dependencies** (what must already be done), and **parallel tracks** (what can run alongside).
- **Owners**:
  - **SB1, SB2** — Senior Backend engineers (OCPP + Python async)
  - **SRE** — Platform / SRE (k8s, Envoy, observability)
  - **QA** — QA + certification engineer
  - **TL** — Tech lead / architect (part-time)
- Tasks tagged `[P]` can be done in parallel within the same week.
- Task IDs (`E0-1`, `E1-3`, …) reference [`02-tasks.md`](./02-tasks.md).
- A week is "done" when its **exit gate** at the bottom passes.

## Team & capacity assumptions

- 4 engineers committed full-time (TL part-time).
- AI assistants used per the rules in [`04-contributing.md`](./04-contributing.md): humans review every MR; tests ship with code.
- Buffer: **2 weeks of slack** built into the plan (W11–W12 have lighter deliverables).
- Working hypothesis: "5 working days per week, no weekends, no on-call disruptions for the team during Phase 0–2."

## Critical-path summary

```
Phase 0 (W0)  ──►  Phase 1 (W1)  ──►  Phase 2 (W2-W3)  ──►  Phase 3 (W4-W5)
                                                                  │
                                                                  ├──► Phase 4 (W6) ──► Phase 5 (W7-W8)
                                                                  │                          │
                                                                  └─ Parallel: OCPP 2.0.1 ───┤
                                                                     (W4 → W16)              │
                                                                                             ▼
                                                                                       Phase 6 (W9)
                                                                                             │
                                                                                             ▼
                                                                                       Phase 7 (W10-W13)
                                                                                             │
                                                                                             ▼
                                                                                       Full rollout
                                                                                       (end of W13)
```

The rollout ramp (Phase 7) is **gate-driven** — moving from W1 → W5 only happens if soak metrics are green. If a wave fails, we hold and roll back. Calendar weeks here are best-case.

---

## W0 — Foundations (Phase 0)

**Goal**: clone-able repo, green CI, AI rules in place, plan approved.

| Task | Owner | Notes |
|---|---|---|
| E0-1: monorepo at `eveys/`, git init | TL | First commit lands docs already written |
| E0-2: Python skeleton (`pyproject.toml`, `Makefile`, `src/eveys_ocpp/`, `tests/`) [P] | SB1 | Empty package; `make tests` green on no code |
| E0-3: GitLab CI (Python 3.13) [P] | SRE | Lint + mypy + pytest on every MR |
| E0-4: pre-commit (black, isort, ruff, mypy --strict) [P] | SB1 | `pre-commit install` works |
| E0-5: Dockerfile (distroless multi-stage) [P] | SRE | Image < 200 MB, runs `python -m eveys_ocpp --version` |
| E0-7: `.editorconfig`, `.gitignore`, `.gitattributes` [P] (extend the root `.gitignore` that already covers docs build output) | SB2 | One quick MR |
| E0-8: implementation plan (this doc) reviewed and merged | TL | This document |
| E0-9: `make doctor` — checks local-dev tool versions per [`07-local-dev-setup.md`](./07-local-dev-setup.md) [P] | SB2 | Aligns "ready to code" check with the setup doc |
| C-1: obtain OCA OCPP 1.6 spec PDF + Errata v4.0 + Security whitepaper Ed.3; place on shared drive [P] | TL | Pre-req for handler spec citations ([`08-ocpp-conformance.md`](./08-ocpp-conformance.md)) |
| C-1a: **begin OCA membership process** (critical-path dependency for OCTT access) [P] | Manager + TL | Without membership, no OCTT, no cert. See [`09-certification-readiness.md`](./09-certification-readiness.md) Stream 1 |
| C-1b: identify OCA-designated test laboratory; scope, price, pre-book W8/W9 slot [P] | Manager | Lab calendars run weeks ahead; do not delay |

**Exit gate** (must all pass to enter W1):

- ✅ `make install && make tests` passes locally on every team member's machine
- ✅ `make doctor` passes locally on every team member's machine — every prerequisite from [`07-local-dev-setup.md`](./07-local-dev-setup.md) is installed at the right version
- ✅ CI is green on a trivial MR
- ✅ Docker image builds and runs
- ✅ TL signs off on plan
- ✅ OCA OCPP 1.6 spec + Errata + Security whitepaper accessible to TL + handler authors + QA (task C-1)
- ✅ OCA membership process initiated (task C-1a) — track until OCTT distribution received
- ✅ Test-lab pre-booked for W8/W9 cert run (task C-1b)

---

## W1 — Protocol skeleton (Phase 1)

**Goal**: real charger (or simulator) completes a transaction end-to-end against `eveys/ocpp` running locally.

| Task | Owner | Depends on |
|---|---|---|
| E1-1: add runtime deps (`ocpp`, `websockets`, `asyncpg`, `alembic`, `sqlalchemy[asyncio]`, `pydantic-settings`, `structlog`) | SB1 | W0 |
| E1-2: settings module (`pydantic-settings`, env-driven) [P] | SB2 | E1-1 |
| E1-3: WS server with subprotocol negotiation | SB1 | E1-1 |
| E1-4: `EveysChargePoint` subclass of `ocpp.v16.ChargePoint` | SB1 | E1-3 |
| E1-10: Postgres schema + Alembic migration (`charge_points`, `transactions`) [P] | SB2 | E1-1 |
| E1-11: structured logging (every line carries `cp_id`, `message_id`, `action`, `direction`) [P] | SB2 | E1-2 |
| E1-12: docker-compose for local dev (Postgres + Redis + Kafka + ClickHouse + service) per [`07-local-dev-setup.md`](./07-local-dev-setup.md) [P] | SRE | E1-10 |
| E1-5: handler `BootNotification` | SB1 | E1-4, E1-10 |
| E1-6: handler `Heartbeat` | SB1 | E1-5 |
| E1-7: handler `StatusNotification` | SB2 | E1-5 |
| E1-8: handler `Authorize` (stub: replies `Accepted` until backend is wired in E3-3) | SB2 | E1-5 |
| E1-9: handler `StartTransaction` / `StopTransaction` | SB1 | E1-8 |
| E1-13: smoke test (sim → service → DB assertions) | QA | E1-9 |

**Exit gate**:

- ✅ Charger simulator completes `BootNotification` → `Authorize` → `StartTransaction` → `StopTransaction`
- ✅ Rows present in `charge_points` and `transactions`
- ✅ All 5 handlers covered by unit + integration tests
- ✅ Coverage ≥ 80%

---

## W2-W3 — Full OCPP 1.6 Core + gRPC + Kafka (Phase 2)

**Goal**: full protocol coverage, multi-pod-aware, internal API contracts in place.

> **Phase 2 exit gate met 2026-05-04** — E2-2..E2-14 all ✅; long-tail E2-1 (remaining ~20 1.6 Core handlers) continues alongside Phase 3 since the infrastructure (Kafka producer hardening, ClickHouse landing, gRPC backward-compat CI, idempotency cache, cross-pod bus) is in place. New handlers plug into the existing pipeline without further platform work.

### W2

| Task | Owner | Depends on |
|---|---|---|
| E2-1: implement remaining ~20 OCPP 1.6 Core actions (split between SB1 and SB2; AI-assisted from `ocpp.v16.call` dataclasses) | SB1, SB2 | W1, **C-2** |
| E2-2: `proto/ocpp_gw/v1/gateway.proto` — gRPC commands [P] | TL + SB1 | W0 |
| E2-3: `proto/events/v1/events.proto` — Kafka envelopes [P] | TL + SB2 | W0 |
| E2-9: Redis online registry (`cp:online:{cp_id}` TTL 120s) [P] | SB2 | W1 |
| C-2: stand up OCTT locally; first BootNotification test case green against running stack | QA | C-1, W1 |
| C-3: OCTT 1.6 Core subset wired into CI (non-blocking initially) | QA + SB2 | C-2 |
| C-4: promote `BootNotification` row in [`08-ocpp-conformance.md`](./08-ocpp-conformance.md) → ✅ | TL | C-3 |
| C-5: promote remaining W1 handler rows (Heartbeat, StatusNotification, Authorize, StartTransaction, StopTransaction) → ✅ | TL + QA | C-4 |

### W3

| Task | Owner | Depends on |
|---|---|---|
| E2-4: gRPC server scaffolding (`grpclib`) | SB1 | E2-2 |
| E2-5: gRPC `RemoteStart` end-to-end | SB1 | E2-4, E2-9 |
| E2-6: remaining gRPC commands (`RemoteStop`, `Reset`, `ChangeConfiguration`, `TriggerMessage`, `UnlockConnector`, `GetChargerStatus`) [P] | SB1, SB2 | E2-5 |
| E2-7: Kafka producer (`aiokafka`) ✅ [P] | SB2 | E2-3 |
| E2-8: wire each handler to publish its event ✅ | SB2 | E2-7 |
| E2-10: cross-pod command bus (Redis pub/sub) ✅ | SB1 | E2-9 |
| E2-11: idempotency cache (`BootNotification`, `StopTransaction`) ✅ [P] | SB2 | E2-9 |
| E2-12: gRPC backward-compat tests (CI fails on field rename) ✅ [P] | QA | E2-6 |
| E2-13: ClickHouse table schemas for `MeterValues` / `StatusNotifications` / `BootNotifications` / `StartTransactions` ✅ [P] | SRE + SB2 | E2-3 |
| E2-14: Kafka → ClickHouse ingestion (sidecar consumer, see ADR-0020) ✅ | SRE + SB2 | E2-13, E2-8 |
| C-3a: wire OCTT Smart Charging subset into CI (after Smart Charging handlers exist) [P] | QA | E2-1 (Smart Charging slice), C-3 |
| C-3b: wire OCTT Reservations + LocalAuthList + RemoteTrigger subsets into CI [P] | QA | C-3 |

**Exit gate**:

- ✅ All ~25 OCPP 1.6 Core actions handled with tests
- ✅ Two-pod local cluster: command issued via gRPC reaches the right pod (cross-pod routing works)
- ✅ Events visible via `kcat`
- ✅ Telemetry events (`MeterValues`, `StatusNotifications`, `BootNotifications`, `StartTransactions`) visible in ClickHouse within seconds of being published to Kafka. Heartbeats are deliberately not in ClickHouse — they're absorbed by the Redis online registry (E2-9) per ADR-0020.
- ✅ Conformance matrix in [`08-ocpp-conformance.md`](./08-ocpp-conformance.md): all six W1 handler rows promoted to ✅; OCTT 1.6 Core subset green in CI
- ✅ Idempotency: replay test produces zero double-effects
- ✅ Coverage ≥ 80% maintained
- ✅ gRPC contract reviewed and frozen for v1

---

## W4-W5 — Platform integration (Phase 3) + start OCPP 2.0.1 parallel track

**Goal**: wire the gateway to the Eveys backend.

Integration shape: REST in both directions plus async webhooks. Specced in [`docs/integration/`](./integration/README.md); decisions in [ADR-0023](./adr/0023-backend-rest-integration.md).

### W4

| Task | Owner | Depends on |
|---|---|---|
| E3-1: backend REST contract finalised + ADR-0023 merged | TL + SB1 | sync with backend dev |
| E3-2: implement `eveys_ocpp.platform.client` (httpx, bearer auth, retry, circuit breaker) | SB1 | E3-1 |
| E3-3: wire `Authorize` handler → `POST /api/eveys/authorize` [P] | SB1 | E3-2 |
| E3-4: cache `Authorize` in Redis (TTL 30s) [P] | SB2 | E3-3 |
| E3-10: publish mock backend (FastAPI, in-repo) for gateway tests [P] | SB2 | E3-1 |
| **Parallel track starts**: P-1 (OCPP 2.0.1 handler scaffolding) | SB2 (50% time) | W2 |

### W5

| Task | Owner | Depends on |
|---|---|---|
| E3-5: wire `StartTransaction` → `POST /api/eveys/sessions/open`; wire `BootNotification` → `POST /api/eveys/charge-points/register` | SB1 | E3-2 |
| E3-6: wire `StopTransaction` → `POST /api/eveys/sessions/close` | SB1 | E3-5 |
| E3-7: gateway-side REST API (read endpoints `/api/v1/...`) [P] | SB2 | E3-1 |
| E3-8: gateway-side REST command endpoints (19 HTTP wrappers) [P] | SB2 | E3-7 |
| E3-9: webhook delivery infrastructure (HMAC-signed, retried) [P] | SB1 | E3-2 |
| Continue P-1: 2.0.1 scaffolding | SB2 (50%) | P-1 (W4) |

**Exit gate**:

- ✅ Charger RFID tap → backend `/authorize` → backend reply forwarded to charger end-to-end.
- ✅ Backend can read MeterValues / status history / transactions via gateway `/api/v1/...`.
- ✅ Backend receives webhook deliveries for `cp.boot` / `cp.status` / `tx.started` / `tx.stopped`.
- ✅ Mock backend published; gateway integration tests run without a live backend.

---

## W6 — Observability + load test (Phase 4)

**Goal**: see what we have; prove it scales to 10k chargers.

| Task | Owner | Depends on |
|---|---|---|
| E4-1: Prometheus metrics (~50 stats: connections, msg/s, gRPC latency, Postgres timing) | SRE | W3 |
| E4-2: 5 Grafana dashboards (fleet, per-pod, per-charger, reconnect storms, transactions) [P] | SRE | E4-1 |
| E4-3: OpenTelemetry tracing on every gRPC + WS message [P] | SB1 | W5 |
| E4-4: Sentry integration [P] | SB2 | W5 |
| E4-5: charger simulator CLI (`eveys-ocpp-sim --count N`) | QA | E1-13 |
| E4-6: load test rig — 10k chargers, 1k tx/min, 1 hour | QA | E4-5 |
| E4-7: reconnect-storm test (kill 50% of pods) [P] | SRE + QA | E4-6 |
| E4-8: SLO definitions + dashboards [P] | TL + SRE | E4-1 |
| C-6: draft three PICS documents (functional, security, performance) per OCA Appendix A | TL + SRE + QA | E4-6 (load test produces perf values) |
| C-8: run CSMS performance measurements (Authorize response time, OCPP response timeout) per Appendix A.2 | QA | E4-6 |
| Continue P-1, P-2: 2.0.1 Device Model | SB2 (50%) | P-1 |

**Exit gate**:

- ✅ Load test: 10k chargers + 1k tx/min, P95 `RemoteStart` < 3s, zero lost transactions
- ✅ Reconnect-storm test: fleet recovers in < 60s, Postgres steady
- ✅ One end-to-end trace shows mobile → BFF → `ocpp-gw` → charger as a single timeline
- ✅ All 5 SLOs measurable from dashboards

---

## W7-W8 — Hardening (Phase 5)

**Goal**: production-ready security and resilience.

### W7

| Task | Owner | Depends on |
|---|---|---|
| E5-1: Envoy edge config (TLS, sticky hash, rate limit, slow-start) | SRE | W6 |
| E5-2: per-IP WS-upgrade rate limit | SRE | E5-1 |
| E5-3: per-charger token-bucket rate cap [P] | SB1 | E2-9 |
| E5-4: sanity-range validation on `MeterValues` [P] | SB2 | W3 |
| E5-7: secrets in vault, mounted as env [P] | SRE | W0 |
| E5-8: `pip-audit` + Dependabot in CI [P] | SRE | W0 |
| Continue P-3: 2.0.1 Core profile end-to-end | SB2 (50%) | P-2 |

### W8

| Task | Owner | Depends on |
|---|---|---|
| E5-5: mTLS between internal services | SRE | E5-7 |
| E5-6: Basic Auth at WS edge + per-CP password store [P] | SB1 | E5-1 |
| E5-9: external pen test | QA + external | E5-1 to E5-7 |
| E5-10: DR drill (kill DB / Redis / pods) [P] | SRE + QA | E4-7 |
| C-3c: wire OCTT Advanced Security subset into CI | QA | E5-5 |
| C-7: **freeze the three PICS documents** for cert run | TL | C-6 |
| **Cert readiness gate review** — confirm all four streams in [`09-certification-readiness.md`](./09-certification-readiness.md) green | TL + Manager | All of: C-1..C-8 done; OCTT 2-week-green in CI; pen-test cleared |
| Continue P-3, P-6: OCTT in CI | QA + SB2 | P-3 |

**Exit gate**:

- ✅ DoS test from one IP gets throttled at the edge
- ✅ Per-charger rate limit triggers backpressure under spam test (no crash)
- ✅ mTLS required between services (manual cert removal blocks calls)
- ✅ Pen test report received; criticals fixed
- ✅ DR drill: service recovers without data loss within target SLOs
- ✅ OCTT 1.6 full (Core + Smart Charging + Reservations + LocalAuthList + RemoteTrigger + Advanced Security) green in CI for two consecutive weeks (tasks C-3..C-3c)
- ✅ Three PICS documents frozen and signed off by TL (task C-7)
- ✅ **Cert-readiness gate (W8 review)** passes — all four streams in [`09-certification-readiness.md`](./09-certification-readiness.md) green; lab visit confirmed for W9 |

---

## W9 — Staging soak (Phase 6)

**Goal**: prove it works for *real* chargers, controlled scope, 7-day soak.

| Task | Owner | Depends on |
|---|---|---|
| E6-1: provision staging cluster (matches prod topology, smaller) | SRE | W8 |
| E6-2: 10 dev-fleet chargers configured to staging URL via `ChangeConfiguration` | SB1 + ops | E6-1 |
| E6-3: per-charger health dashboards | SRE | E4-2 |
| E6-4: rollback runbook + drill (disconnect a charger in < 2 min) [P] | TL + SRE | E6-2 |
| Continue P-3, P-4: 2.0.1 Authorization profile | SB2 (50%) | P-3 |

**Exit gate** (the big one — cannot enter Phase 7 without):

- ✅ 10 real chargers running on staging `ocpp-gw` for **7 consecutive days**
- ✅ Zero P0/P1 incidents during soak
- ✅ Per-charger error rate within target SLO
- ✅ Rollback drill performed under 2 minutes
- ✅ All P0 metrics (transactions integrity, command success, connection availability) within SLO

> **If this gate fails, hold W10 and triage. Do not move chargers to production until staging is stable.**

---

## W10-W13 — Production rollout (Phase 7)

**Goal**: bring the production fleet onto `ocpp-gw` in safe, gated waves. **Gate-driven** — pace is set by metrics, not the calendar.

### W10 — Wave 1 + Wave 2

| Task | Owner | Wave gate |
|---|---|---|
| E7-1: per-`cp_id` allowlist controlling which chargers connect | SRE | (prereq) |
| E7-3: nightly reconciliation report [P] | QA | (prereq) |
| E7-4: **Wave 1 — 10 chargers** | TL + ops | 7 days, error rate < 0.1%, zero discrepancies |
| E7-5: **Wave 2 — 100 chargers** (start at end of W10 if W1 green) | TL + ops | 5 days, same gates |
| Continue P-4, P-5: 2.0.1 Smart Charging | SB2 (50%) | P-4 |

### W11 — Wave 3

| Task | Owner | Wave gate |
|---|---|---|
| E7-6: **Wave 3 — 1,000 chargers** | TL + ops | 3 days, error rate < 0.1%, zero discrepancies |
| Continue P-5, P-6: full OCTT pass (Core) | QA + SB2 | OCTT Core profile passes |
| Buffer: pen-test follow-up fixes if any | SB1 | E5-9 leftovers |

### W12 — Wave 4

| Task | Owner | Wave gate |
|---|---|---|
| E7-7: **Wave 4 — 10,000 chargers** | TL + ops | 3 days, error rate < 0.1%, zero discrepancies |
| Buffer: address any issues from Wave 1–3 soak | SB1, SB2 | per-incident |
| Continue P-5: Smart Charging tests | SB2 | P-5 |

### W13 — Wave 5

| Task | Owner | Done when |
|---|---|---|
| E7-8: **Wave 5 — full rollout** (all remaining chargers) | TL + ops | 100% on `ocpp-gw` |
| Comms to internal stakeholders (CS, ops) | TL | per-team confirmation |
| Lessons-learned retrospective | All | Document in `docs/postmortems/` |

**Exit gate (project-level)**:

- ✅ 100% of fleet served by `ocpp-gw`
- ✅ All five SLOs at or above target during the rollout window
- ✅ Zero data discrepancies in nightly reconciliation
- ✅ Retrospective written

---

## Beyond W13 — OCPP 2.0.1 + OCTT certification (Parallel track continued)

The 2.0.1 work continues on its own cadence after the 1.6 rollout.

| Week (~) | Deliverable | Notes |
|---|---|---|
| W14 | P-5 complete: full Core + Authorization + LocalAuthList + Smart Charging profiles | OCTT all relevant profiles pass |
| W15 | OCTT certification submission to OCA | Submission package ready |
| W16 | Certification feedback loop, retest as needed | Typical: 1 round of fixes |
| W17–W18 | Certification awarded; first real 2.0.1 charger onboarded in staging | Coordinate with vendor |
| W19+ | Production rollout for 2.0.1 chargers | Lighter; reuses Phase 7 tooling |

---

## Cross-cutting (every week)

These don't stop and don't get a single owner — the team keeps them green continuously:

| Task | Owner | Cadence |
|---|---|---|
| X-1: coverage ≥ 80% on `src/eveys_ocpp/` | All | Every MR |
| X-2: docs updated alongside code | All | Every MR |
| X-3: dependency upgrades | SRE | Quarterly sprint |
| X-4: chaos drill | SRE + QA | Quarterly |
| X-5: post-mortem within 48h of incident | TL | Per incident |

---

## Dependencies & critical path

The single critical path through the plan:

```
E0-2 → E1-1 → E1-3 → E1-4 → E1-5 → E1-9
                                     ↓
                                    E2-1 (all 1.6 actions)
                                     ↓
                                    E2-2 (proto) → E2-4 → E2-5 → E2-6 (gRPC commands)
                                     ↓
                                    E2-7 → E2-8 (Kafka producer + wiring)
                                     ↓
                                    E3-2 → E3-5 → E3-6 (platform integration)
                                     ↓
                                    E4-6 (load test)
                                     ↓
                                    E5-1 (Envoy) → E5-9 (pen test)
                                     ↓
                                    E6-2 (staging chargers) → 7-day soak
                                     ↓
                                    E7-4 → E7-5 → E7-6 → E7-7 → E7-8 (wave-by-wave)
```

Anything *not* on this critical path is parallelizable and assigned `[P]` in the weekly tables.

---

## Parallelization budget

| Track | Time investment | Owner |
|---|---|---|
| Main track (1.6 to GA) | 14 weeks full-time | SB1 + SB2 (rotating) + SRE + QA |
| OCPP 2.0.1 + OCTT | 12 weeks at 50% from W4 | SB2 |
| Platform-team integrations (mobile, auth, session teams) | Their own backlog, gated by E3-1 | Their teams |
| Documentation | Continuous, AI-assisted | All |

We're banking that **AI compression** delivers ~3× on boilerplate-heavy work (handlers, gRPC, manifests, tests). If that estimate is wrong, the buffer in W11–W12 absorbs ~1 week of slip.

If we slip more than a week, the recovery options (in priority order):

1. Reduce 2.0.1 parallel-track to 25% time (delays cert, doesn't delay 1.6 rollout).
2. Drop or defer "nice-to-have" hardening (e.g. mTLS pushed to post-GA).
3. Hold a wave in Phase 7 until red metrics clear (does not slip GA — slips full rollout).

We do **not** cut: tests, security review, OCTT compliance, the 7-day staging soak.

---

## Decision points (when leadership weighs in)

| When | Decision needed | Who decides |
|---|---|---|
| End of W0 | Approve plan as-is or adjust scope | Manager + TL |
| End of W3 | gRPC contracts frozen for v1 | TL + platform team |
| End of W6 | Load-test results acceptable to proceed | TL + SRE |
| End of W8 | Pen-test report acceptable; security profile signed off | TL + Security |
| End of W9 | Staging soak passes — green-light Phase 7 | TL + Manager |
| Each wave in W10–W13 | Soak gates passed → next wave | TL + ops lead |
| End of W13 | Project complete; full rollout greenlit | Manager |

---

## Risk-adjusted forecast

| Scenario | Outcome |
|---|---|
| **Plan-on-rails** (no slip) | Full 1.6 rollout end of W13 (~14 weeks) |
| **One bad week** (typical) | Rollout slips to W14 or W15 — buffer absorbs |
| **Vendor interop bug at scale** (Wave 3 fails) | Hold rollout 1–2 weeks, fix, resume — total slip ~2 weeks |
| **OCTT first-time failure** (likely) | 2.0.1 GA slips to W18–W20; **does not affect 1.6 rollout** |
| **Pen test surfaces a P0** | Hold Phase 6 until fixed; slip 1–2 weeks |
| **Platform team contract churn** (E3-1 keeps changing) | Mock-server (E3-9) keeps us moving; cross-team integration slips, not the gateway itself |

> **The 4-person team + AI compression hypothesis is the biggest unverified assumption in this plan.** The first major checkpoint is **end of W3**: if all ~25 OCPP 1.6 actions are not handled with tests by then, the AI-velocity bet is failing and we re-plan.

---

## What "done" looks like at the end of W13

- ✅ Every charger in the fleet is connected to `eveys/ocpp`.
- ✅ All five SLOs at target on the 30-day rolling dashboard.
- ✅ Zero data discrepancies in nightly reconciliation for the rollout window.
- ✅ Pen-test critical and high findings are closed.
- ✅ OCPP 2.0.1 + OCTT track on track for cert in W15–W16.
- ✅ Retrospective document published.
- ✅ Mobile, auth, and operator teams are unblocked and using the new contracts.

---

## What happens after W13

- W14+: ongoing operations, 2.0.1 rollout for new vendor firmware, OCPI roaming and ISO 15118 are *future projects*.

---

## Update protocol

This plan is a living document. Edits go through MR like everything else.

- **Date slipped**: update the affected week, add a one-line "slipped because…" note. Don't silently shift.
- **Scope changed**: ADR if it's load-bearing; otherwise MR explaining why.
- **Re-plan**: keep this version in `docs/06-implementation-plan-v1.md` and start `06-implementation-plan-v2.md`. Don't lose the history.
