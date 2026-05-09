<p align="center">
  <a href="https://eveys.com">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/_static/eveys-white.svg">
      <img src="docs/_static/eveys-white.svg" alt="Eveys" width="220">
    </picture>
  </a>
</p>

<p align="center">
  <a href="https://github.com/eveys-mobility/OCPP/actions/workflows/quality.yml"><img alt="quality" src="https://github.com/eveys-mobility/OCPP/actions/workflows/quality.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/eveys-mobility/OCPP/actions/workflows/tests.yml"><img alt="tests" src="https://github.com/eveys-mobility/OCPP/actions/workflows/tests.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/eveys-mobility/OCPP/actions/workflows/e2e.yml"><img alt="e2e" src="https://github.com/eveys-mobility/OCPP/actions/workflows/e2e.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/eveys-mobility/OCPP/actions/workflows/compose-smoke.yml"><img alt="compose-smoke" src="https://github.com/eveys-mobility/OCPP/actions/workflows/compose-smoke.yml/badge.svg?branch=main"></a>
  <a href="https://github.com/eveys-mobility/OCPP/actions/workflows/security.yml"><img alt="security" src="https://github.com/eveys-mobility/OCPP/actions/workflows/security.yml/badge.svg?branch=main"></a>
  <br>
  <a href="./LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg"></a>
  <a href="https://www.python.org/downloads/"><img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13%2B-blue"></a>
  <img alt="OCPP 1.6 / 2.0.1" src="https://img.shields.io/badge/OCPP-1.6%20%7C%202.0.1-blueviolet">
  <a href="https://github.com/eveys-mobility/OCPP/issues"><img alt="GitHub issues" src="https://img.shields.io/github/issues/eveys-mobility/OCPP"></a>
  <a href="https://github.com/eveys-mobility/OCPP/pulls"><img alt="GitHub pull requests" src="https://img.shields.io/github/issues-pr/eveys-mobility/OCPP"></a>
  <a href="https://github.com/eveys-mobility/OCPP/commits/main"><img alt="Last commit" src="https://img.shields.io/github/last-commit/eveys-mobility/OCPP"></a>
</p>

# eveys-mobility/ocpp

OCPP 1.6 / 2.0.1 CSMS gateway. Terminates charger WebSockets,
schema-validates and dispatches OCPP messages, and exposes a
backend-facing contract over REST, gRPC, Kafka, and webhooks.

Python 3.13 + asyncio + uvloop. Horizontally scaled out of the box:
Envoy ring-hash on `cp_id`, Redis-backed online registry and
cross-pod command bus, idempotency cache for inbound replays,
Kafka-tailing ClickHouse ingestor for telemetry, graceful drain on
`SIGTERM`. Apache-2.0.

**Status.** OCPP 1.6 Core profile complete; 2.0.1 in progress under
`handlers/v201/`. Conformance matrix:
[`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md). OCTT
certification target:
[`docs/09-certification-readiness.md`](./docs/09-certification-readiness.md).

> New to this repo? [Try it in 5 minutes](#try-it-in-5-minutes).

---

## Table of contents

- [Surfaces](#surfaces)
- [Architecture](#architecture)
- [Try it in 5 minutes](#try-it-in-5-minutes)
- [Reading this repo by role](#reading-this-repo-by-role)
- [Terms (OCPP-specific)](#terms-ocpp-specific)
- [The OpenAPI / Swagger UI](#the-openapi--swagger-ui)
- [Make targets — full reference](#make-targets--full-reference)
- [Multi-pod](#multi-pod)
- [Other features](#other-features)
- [Stack](#stack)
- [Configuration](#configuration)
- [Project layout](#project-layout)
- [Tests](#tests)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Security](#security)
- [Support](#support)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## Surfaces

The service exposes five surfaces. All five run inside a single
event loop on one process, supervised by `asyncio.TaskGroup` (see
`src/eveys_ocpp/__main__.py`).

| Surface | Bind | Direction | Purpose |
|---|---|---|---|
| OCPP WebSocket | `:9000` (`:19000` under compose) | charger → gateway | OCPP-J 1.6 / 2.0.1 over WSS, subprotocol negotiated via `Sec-WebSocket-Protocol`. Every inbound message is validated against the [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) bundled JSON Schemas before dispatch. |
| REST | `:8080` (`/api/v1/...`) | platform → gateway | Read state (charge-points, transactions, reservations, charging profiles, time-series) and issue CSMS-initiated commands (`RemoteStart`, `RemoteStop`, `Reset`, `ReserveNow`, `SetChargingProfile`, …). 28 routes, full OpenAPI 3.1 spec. |
| gRPC | `:50051` | platform → gateway | Same command surface as REST, lower overhead, typed clients. Generated stubs under `src/eveys_ocpp/_generated/` from `proto/ocpp_gw/v1/gateway.proto`. |
| Kafka producer | `EVEYS_OCPP_KAFKA_BROKERS` | gateway → bus | Versioned event envelope (`proto/events/v1/events.proto`), `cp_id` partition key. Topics: `cp.boot`, `cp.status`, `cp.meter`, `tx.started`. |
| Webhooks | gateway-initiated HTTP POST | gateway → backend | Kafka-tailing dispatcher with HMAC-signed envelopes and exponential-backoff retry ([ADR-0027](./docs/adr/0027-webhook-delivery.md)). Push counterpart to Kafka consumer-group subscription. |

The gateway also calls the configured backend on the OCPP hot path —
`POST /api/eveys/authorize`, `/sessions/open`, `/sessions/close`,
`/charge-points/register` — over `httpx`. Asymmetric envelope
(outbound enveloped, inbound raw); contract in
[`docs/integration/`](./docs/integration/) and [ADR-0023](./docs/adr/0023-backend-rest-integration.md).

---

## Architecture

```
                            Chargers (fleet)
                                  │ wss:// OCPP-J 1.6 / 2.0.1
                                  ▼
                          ┌───────────────┐
                          │ Envoy edge    │  TLS termination,
                          │ (RING_HASH on │  consistent-hash on URL :path
                          │  :path)       │  → cp_id stickiness
                          └───────┬───────┘
                                  │ ws://  (HTTP/1.1, mTLS upstream)
                  ┌───────────────┼───────────────┐
                  ▼               ▼               ▼
          ┌───────────┐   ┌───────────┐   ┌───────────┐
          │ ocpp-gw   │   │ ocpp-gw   │   │ ocpp-gw   │   ← this service,
          │  pod 1    │   │  pod 2    │   │  pod N    │     N replicas
          └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
                │               │               │
        ┌───────┴───────┬───────┴───────┬───────┴───────┐
        ▼               ▼               ▼               ▼
   ┌─────────┐    ┌─────────┐    ┌──────────┐   ┌─────────────┐
   │Postgres │    │  Redis  │    │  Kafka   │   │ Backend     │
   │         │    │ • online│    │          │   │ (peer       │
   │relational│   │   reg.  │    │event     │   │  service,   │
   │ state   │    │ • pub/  │    │firehose  │   │  per        │
   │         │    │   sub   │    │          │   │  contract)  │
   │         │    │   bus   │    │          │   └─────────────┘
   │         │    │ • idem. │    │          │     ↑      ↑
   │         │    │   cache │    │          │     │      │
   └─────────┘    └─────────┘    └────┬─────┘     │      │
                                      │           │      │
                                      ▼           │      │
                              ┌──────────────┐    │      │
                              │ ClickHouse   │    │      │
                              │ ingestor     │────┘      │
                              │ (sidecar)    │  webhooks │
                              └──────┬───────┘   (HMAC)  │
                                     ▼                   │
                              ┌──────────────┐           │
                              │  ClickHouse  │           │
                              │ (time-series)│           │
                              └──────────────┘           │
                                                         │
        Hot path (Authorize / sessions/open / close) ────┘
        via httpx, asymmetric envelope
```

---

## Try it in 5 minutes

You'll bring up the gateway plus its data plane (Postgres, Redis,
Kafka, ClickHouse) on your laptop and click around the API in
Swagger.

### Prerequisites

- macOS or Linux. Windows works under WSL2.
- Python **3.13+** — `brew install python@3.13` on macOS.
- [`uv`](https://github.com/astral-sh/uv) — `brew install uv`.
- Docker + Docker Compose.
- `make`.

If anything is missing, `make doctor` will tell you exactly what.

### Run it

```bash
git clone git@github.com:eveys-mobility/OCPP.git
cd OCPP

make install        # creates .venv, installs deps, generates proto stubs
                    # (~ 60 s on a fresh machine)

make compose-up     # starts Postgres + Redis + Kafka + ClickHouse + the gateway
                    # AND runs the database migrations
                    # (~ 30–60 s; Kafka is the slow one on first boot)
```

When that finishes, you have:

| Surface | URL |
|---|---|
| OCPP WebSocket (where chargers connect) | `ws://localhost:19000/<cp_id>` (subprotocol: `ocpp1.6`) |
| Gateway REST | `http://localhost:8080/api/v1/...` |
| Swagger UI (when enabled — see below) | `http://localhost:8080/api/v1/docs` |
| Prometheus metrics | `http://localhost:9100/metrics` |
| gRPC | `localhost:50051` |

> The WS port is `19000` (not `9000`) under compose because port
> `9000` is reserved by ClickHouse's native protocol on the host.
> Outside compose, the gateway's WS port is `9000`.

### Hit the API in Swagger

```bash
# 1. Bring the stack up with the OpenAPI surface enabled.
EVEYS_OCPP_REST_OPENAPI_ENABLED=true make compose-up

# 2. Get the bearer token Swagger needs to "Authorize".
make get-token            # prints a token, e.g. "dev-token"
make get-token | pbcopy   # or: copy straight to clipboard (macOS)

# 3. Open Swagger UI.
open http://localhost:8080/api/v1/docs
#    → click "Authorize" → paste the token → "Try it out" anywhere.
```

That's a working OCPP CSMS on your laptop. To make it actually do
something, point an OCPP charger simulator at
`ws://localhost:19000/<cp_id>` and watch the events flow.

### Tear it down

```bash
make compose-down              # stop containers, KEEP data
make compose-down-volumes      # stop AND wipe data — asks first
```

---

## Reading this repo by role

Different readers need different paths:

- **"I just want to see it work."** → [Try it in 5 minutes](#try-it-in-5-minutes).
- **"I'm a backend dev integrating with the gateway."** → start
  with [`docs/integration/`](./docs/integration/), then
  [`docs/15-openapi.md`](./docs/15-openapi.md).
- **"I'm onboarding a hardware charger."** →
  [`docs/12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md).
- **"I want to contribute code."** → [`CONTRIBUTING.md`](./CONTRIBUTING.md),
  then [`docs/03-coding-standards.md`](./docs/03-coding-standards.md),
  then [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md)
  for the *why* behind every load-bearing decision.
- **"I'm operating this in production."** →
  [`docs/14-slos.md`](./docs/14-slos.md),
  [`docs/16-dr-runbook.md`](./docs/16-dr-runbook.md),
  [`docs/17-sizing.md`](./docs/17-sizing.md),
  [`docs/18-rollback-runbook.md`](./docs/18-rollback-runbook.md).
- **"What's already done? What's next?"** →
  [`docs/01-roadmap.md`](./docs/01-roadmap.md) and
  [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md)
  (per-test-case OCPP coverage matrix).

---

## The OpenAPI / Swagger UI

The gateway's REST API is fully described as **OpenAPI 3.1**:
28 routes covering health checks, charge-point listings, transactions,
reservations, charging profiles, time-series queries (`MeterValues`,
status history), and 19 commands.

You can interact with it three ways:

| Surface | Where | When to use |
|---|---|---|
| **Runtime Swagger UI** | `/api/v1/docs` on a running gateway | Click "Try it out" against a real backend. **Off by default**; flip on with `EVEYS_OCPP_REST_OPENAPI_ENABLED=true`. |
| **Runtime ReDoc** | `/api/v1/redoc` | Same data, different renderer. |
| **Static spec** | [`docs/api/openapi.yaml`](./docs/api/openapi.yaml) and `openapi.json` | Import into Postman / Insomnia / external tooling. Regenerated from the live FastAPI app via `make openapi-export`; CI fails on drift. |
| **Static Swagger UI site** | `make -C docs swagger-serve` | Browse the contract without a running gateway — pure HTML/CSS/JS, copyable to any web host. |

### Walkthrough — get a token and hit Swagger

```bash
# 1. Bring the local stack up with the OpenAPI surface enabled.
EVEYS_OCPP_REST_OPENAPI_ENABLED=true make compose-up

# 2. Print a bearer token. `make get-token` reads
#    EVEYS_OCPP_REST_INBOUND_TOKENS from your shell env first, then
#    falls back to the repo-root .env file. Output is just the token,
#    safe to pipe.
make get-token            # → dev-token
make get-token | pbcopy   # → copies to clipboard (macOS)

# 3. Open Swagger UI and click "Authorize".
open http://localhost:8080/api/v1/docs
```

If `EVEYS_OCPP_REST_INBOUND_TOKENS` is not set anywhere,
`make get-token` tells you exactly what to do:

```
ERROR: EVEYS_OCPP_REST_INBOUND_TOKENS is not set in shell env or .env
       Add a value to .env (e.g. EVEYS_OCPP_REST_INBOUND_TOKENS=dev-token)
       and re-run.
```

`.env.example` ships a sensible default to copy.

### A note on production

`EVEYS_OCPP_REST_OPENAPI_ENABLED` is **off by default**. The gateway
deliberately does not self-publish a discoverable schema in
production — flipping it on widens the attack surface (anyone with a
valid bearer token can read the full schema and discover endpoints).
For prod deployments, prefer the static spec at
`docs/api/openapi.{json,yaml}` shared via internal channels. A
boot-time `WARNING` log fires when the toggle is on; grep for
`rest_openapi.enabled` to catch a misconfigured deploy. Full rationale
in [`docs/15-openapi.md`](./docs/15-openapi.md) and ADR-0026.

---

## Make targets — full reference

`make help` prints these from the live Makefile. Same content,
grouped by concern:

### Environment

| Target | What it does |
|---|---|
| `make doctor` | Check that local-dev tools (Python 3.13, uv, Docker, …) are installed. Shells out to `scripts/doctor.sh`. |
| `make install` | Create `.venv/` via `uv`, install runtime + dev deps as an editable wheel, regenerate gRPC stubs from `proto/`, install the pre-commit hooks. Idempotent. |
| `make protoc` | Regenerate `src/eveys_ocpp/_generated/` from the `.proto` files. Run when a `.proto` changes; `make install` already calls it. |

### Code quality

| Target | What it does |
|---|---|
| `make format` | Apply `isort` + `black` to `src/`, `tests/`, `scripts/`. |
| `make lint` | Run `ruff check` on `src/`, `tests/`, `scripts/`. |
| `make types` | Run `mypy --strict` on `src/`. |
| `make tests` | Full pre-commit gate: lint + types + `pytest` with the ≥ 80 % coverage gate. |
| `make audit` | Run `pip-audit` against the resolved venv (PyPI advisory DB / OSV). Mirrors the CI security workflow so you can repro a CVE finding before pushing. |
| `make precommit` | Run every pre-commit hook against every file (no commit needed). |

### Local stack (data plane)

| Target | What it does |
|---|---|
| `make compose-up` | Start Postgres 16 + Redis 7 + Kafka (KRaft) + ClickHouse 24 + the gateway. **Bakes in `make migrate`** — Postgres + ClickHouse schemas are applied before the target returns, so a fresh `compose-up` produces a usable stack, not a half-deployed one. |
| `make compose-wait` | Block until every container with a healthcheck reports `healthy` (timeout 120 s). |
| `make compose-status` | Show container health (`docker compose ps`). |
| `make compose-down` | Stop containers, **keep** data volumes. |
| `make compose-down-volumes` | Stop containers **and wipe** Postgres / Kafka / ClickHouse data volumes. Asks for confirmation. |
| `make build-image` | Build the production-shaped container image as `eveys-ocpp:dev` from `deploy/Dockerfile` (~ 170 MB, distroless base). |
| `make migrate` | Apply both Postgres (Alembic) **and** ClickHouse migrations against the running stack. Idempotent. |
| `make pg-migrate` | Postgres migrations only (`alembic upgrade head`). |
| `make ch-migrate` | ClickHouse migrations only (reads SQL from `src/eveys_ocpp/clickhouse/ddl/`, skips files already in `schema_migrations`). |
| `make get-token` | **Print a bearer token** from `EVEYS_OCPP_REST_INBOUND_TOKENS` (env or `.env`). Quiet output, pipe-friendly. Use it to authorise the Swagger UI. |
| `make grafana-up` | Bring up the dev-only Grafana + Prometheus sidecar overlay, joined to the same compose network. Grafana on `:3000` (anonymous Admin), Prometheus on `:9090`. Dashboards land under `eveys/ocpp`; the per-charger drill-down reads from ClickHouse. |
| `make grafana-down` | Stop the Grafana + Prometheus sidecar (data volumes kept). |
| `make mock-backend` | Boot the dev-only mock implementing the backend REST contract (`docs/integration/01-backend-rest-contract.md`). Bind `0.0.0.0:9200`, token `dev-token`, accepts all `id_tag` values. Override via `MOCK_BACKEND_*` env vars. |

### Tests (tiered)

| Target | What it does |
|---|---|
| `make tests` | **Tier 1** — unit + lint + types. The fast inner loop. |
| `make e2e` | **Tier 2** — bring the data plane up, apply migrations, run `tests/e2e/` against a real Postgres/Redis/Kafka/ClickHouse, tear down. |
| `make compose-smoke` | **Tier 3** — build the production-shaped image, run `tests/compose_smoke/` against the actual compose stack, capture container logs as artifacts. Catches the bug class that ships green through unit + integration but breaks `docker compose up` (ADR-0024). |

### Docs

| Target | What it does |
|---|---|
| `make docs` | Build the docs site (Sphinx + MyST). Output under `docs/_build/html/`. |
| `make docs-clean` | Remove `docs/_build/`. |
| `make -C docs swagger` | Build a static Swagger UI site rendered from the committed `docs/api/openapi.json`. Output under `docs/_build/swagger/`. |
| `make -C docs swagger-serve` | Build (if needed) and serve the static Swagger UI on `http://localhost:8000`. Override port via `SWAGGER_PORT=…`. |

### Generated artifacts (config + OpenAPI)

| Target | What it does |
|---|---|
| `make config-export` | Regenerate `docs/11-configuration-reference.md` and `.env.example` from the `Settings` Pydantic model. |
| `make config-export-check` | Fail if either of those files is out of date. CI gate (ADR-0025). |
| `make config-schema` | Print `Settings` as JSON Schema. Plumbing for Helm validators / operator UIs / CLI `--help`. Pipe to a file when you need it persisted. |
| `make openapi-export` | Regenerate `docs/api/openapi.{json,yaml}` from the live FastAPI app. |
| `make openapi-export-check` | Fail if the committed OpenAPI files drift from the FastAPI app. CI gate. |

### Cleanup

| Target | What it does |
|---|---|
| `make clean` | Remove `.pytest_cache`, `.mypy_cache`, `.ruff_cache`, coverage artifacts, `__pycache__/`, `docs/_build/`. |
| `make distclean` | `clean` + remove `.venv/` and `docs/.venv/`. |

---

## Multi-pod

The gateway is designed for horizontal scale-out. N replicas run
simultaneously, each holding a slice of the fleet's chargers. Five
mechanisms make that work:

**Sticky charger routing — Envoy `RING_HASH` on `:path`.** The
`cp_id` is encoded in the WS URL path. Envoy's consistent-hash LB
policy hashes the path and routes the connection to the same upstream
pod across reconnects. This keeps the per-pod idempotency cache and
in-process `ChargePoint` state warm. Rationale and rejected
alternatives (Nginx OSS, ALB, NLB, HAProxy, K8s `sessionAffinity`):
[ADR-0007](./docs/adr/0007-envoy-as-the-load-balancer.md).

**Online registry — Redis.** On WS accept, each pod writes
`cp:online:{cp_id} → pod_id` with a TTL refresh; tombstoned on
disconnect. O(1) lookup answers "which pod owns `CP_ACME_42`
right now?" without scanning. `src/eveys_ocpp/registry.py`.

**Cross-pod command bus — Redis pub/sub.** Off-pod RPCs publish on
`cp:cmd:{cp_id}`; every pod pattern-subscribes to `cp:cmd:*` and the
owning pod dispatches to its in-process `ChargePoint`. The OCPP
response is published on `cp:reply:{request_id}`; the originating
pod resolves its in-flight future. Sub-second overhead under the
30 s gRPC ceiling. JSON envelope with `v` field for forward-version
skew. `src/eveys_ocpp/bus.py`; full design with rejected alternatives
(NATS, Kafka request/reply, pod-to-pod gRPC, per-charger
subscriptions): [ADR-0016](./docs/adr/0016-cross-pod-command-bus.md).

**Idempotency cache — Redis.** Inbound `BootNotification` and
`StopTransaction` are keyed by `MessageId`; replays return the cached
response. Prevents duplicate transactions / duplicate boot acks
under flaky links. [ADR-0017](./docs/adr/0017-idempotency-cache.md).

**Graceful drain.** `GET /api/v1/ready` is auth-exempt. On `SIGTERM`
it flips to 503; Envoy's health check pulls the pod from rotation
before the process exits. Reconnecting chargers land on a healthy
sibling pod within the drain window. End-to-end test:
`tests/e2e/test_two_pod_dispatch.py`.

ADR index for the full set of decisions:
[`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md).

---

## Other features

**ClickHouse ingestion sidecar.** Telemetry goes to Kafka with a
versioned envelope, partitioned by `cp_id`. A dedicated sidecar
(`src/eveys_ocpp/clickhouse/ingestor.py`) tails `cp.boot`,
`cp.status`, `cp.meter`, `tx.started` and inserts into ClickHouse
tables. The gateway's hot path never blocks on the time-series
store. Heartbeats are absorbed by the Redis registry — no
time-series row written. [ADR-0020](./docs/adr/0020-clickhouse-ingestion-sidecar.md).

**Webhook dispatcher.** `src/eveys_ocpp/webhooks/dispatcher.py`
tails Kafka and POSTs HMAC-SHA256-signed envelopes to per-event-type
URLs configured per operator, with exponential-backoff retry and a
DLQ for poison messages. [ADR-0027](./docs/adr/0027-webhook-delivery.md).

**Smart Charging.** Charger-side resolver, gateway-side profile
mirror — the gateway records `SetChargingProfile` payloads but
never recomputes schedules. [ADR-0022](./docs/adr/0022-smart-charging-charger-side-resolver.md).

**Reservations.** Charger-side authority with a gateway-side mirror
of `ReserveNow` / `CancelReservation` outcomes. [ADR-0021](./docs/adr/0021-reservations-charger-authority.md).

**Observability.**
- Logs: `structlog` → JSON; every line carries `cp_id` and
  `request_id`. CI gate against reserved-`LogRecord`-key collisions.
- Metrics: Prometheus on `/metrics:9100`. SLO rules in
  `deploy/prometheus/rules.yml` validated by a unit test.
- Traces: OpenTelemetry SDK, OTLP/gRPC exporter. Tracer is no-op
  until `configure_tracing()` runs at boot.
- Errors: Sentry. Inactive when `EVEYS_OCPP_SENTRY_DSN` is empty
  (no transport, no breadcrumbs, no monkey-patches).

**Security.**
- `EVEYS_OCPP_REST_INBOUND_TOKENS` defaults to empty — REST rejects
  every request until tokens are configured (production-safe default).
- WS edge Basic Auth: per-charger bcrypt-hashed passwords;
  fail-closed on DB error (#121).
- TLS termination at Envoy; mTLS between Envoy and the gateway
  upstream. [ADR-0011](./docs/adr/0011-internal-mtls.md).
- OCPP 1.6 Security Whitepaper coverage: §4.5 cert management
  (TC_073/074/075/076), §4.4 `SignedUpdateFirmware` signature
  verification (TC_080/081), `SecurityEventNotification` log.
- `pip-audit` runs in CI (`security.yml` workflow) and locally via
  `make audit`.

---

## Terms (OCPP-specific)

Quick reference for OCPP vocabulary used throughout the codebase:

| Term | Definition |
|---|---|
| OCPP | Open Charge Point Protocol; charger ↔ CSMS messaging spec by the [Open Charge Alliance](https://www.openchargealliance.org/). This service implements **OCPP-J 1.6** and **OCPP 2.0.1**. |
| CSMS | Charging Station Management System — the OCPP server role. This service is a CSMS. |
| CP / charge point | A single physical charger. |
| `cp_id` | Charge-point identifier; carried on every log line, metric, and DB row. The WS URL path (`/<cp_id>`) is the consistent-hash key for Envoy. |
| OCPP CALL | One protocol message — `BootNotification`, `Authorize`, `StartTransaction`, `MeterValues`, `StopTransaction`, `StatusNotification`, etc. |
| Subprotocol | `Sec-WebSocket-Protocol` value: `ocpp1.6` or `ocpp2.0.1`. Negotiated at WS handshake. |
| `id_tag` | RFID / app token presented by a driver. The gateway calls the backend's `/authorize` to get an `Accepted` / `Blocked` / `Expired` decision. |
| OCTT | OCPP Compliance Testing Tool, by the OCA. Certification target — see [`docs/09-certification-readiness.md`](./docs/09-certification-readiness.md). |

---

## Stack

The pinned floors live in [`pyproject.toml`](./pyproject.toml). All
licenses are OSI-approved permissive or weak-copyleft; nothing
copyleft-strong is pulled into the runtime.

### Language and runtime

| Component | Version | License | Source |
|---|---|---|---|
| Python | ≥ 3.13 | PSF-2.0 | [python.org](https://www.python.org/) |
| uvloop | ≥ 0.21 | Apache-2.0 / MIT (dual) | [MagicStack/uvloop](https://github.com/MagicStack/uvloop) |

### OCPP protocol

| Component | Version | License | Source |
|---|---|---|---|
| `ocpp` (mobilityhouse) | ≥ 2.1, < 2.2 | MIT | [mobilityhouse/ocpp](https://github.com/mobilityhouse/ocpp) |
| `websockets` | ≥ 13.1 | BSD-3-Clause | [python-websockets/websockets](https://github.com/python-websockets/websockets) |

### Persistence

| Component | Version | License | Source |
|---|---|---|---|
| PostgreSQL | 16 (compose) | PostgreSQL License | [postgresql.org](https://www.postgresql.org/) |
| `asyncpg` | ≥ 0.30 | Apache-2.0 | [MagicStack/asyncpg](https://github.com/MagicStack/asyncpg) |
| `SQLAlchemy[asyncio]` | ≥ 2.0.36 | MIT | [sqlalchemy.org](https://www.sqlalchemy.org/) |
| `alembic` | ≥ 1.14 | MIT | [alembic.sqlalchemy.org](https://alembic.sqlalchemy.org/) |
| ClickHouse | 24 (compose) | Apache-2.0 | [clickhouse.com](https://clickhouse.com/) |
| `asynch` | ≥ 0.2 | Apache-2.0 | [long2ice/asynch](https://github.com/long2ice/asynch) |

### Messaging and coordination

| Component | Version | License | Source |
|---|---|---|---|
| Redis | 7 (compose) | RSALv2 / SSPL (server) — clients are independent | [redis.io](https://redis.io/) |
| `redis` (Python client) | ≥ 5.2 | MIT | [redis/redis-py](https://github.com/redis/redis-py) |
| Apache Kafka | KRaft (compose) | Apache-2.0 | [kafka.apache.org](https://kafka.apache.org/) |
| `aiokafka` | ≥ 0.12 | Apache-2.0 | [aio-libs/aiokafka](https://github.com/aio-libs/aiokafka) |

### Transports and APIs

| Component | Version | License | Source |
|---|---|---|---|
| `grpclib` | ≥ 0.4.7 | BSD-3-Clause | [vmagamedov/grpclib](https://github.com/vmagamedov/grpclib) |
| `protobuf` | ≥ 5.28 | BSD-3-Clause | [protocolbuffers/protobuf](https://github.com/protocolbuffers/protobuf) |
| `fastapi` | ≥ 0.115 | MIT | [tiangolo/fastapi](https://github.com/tiangolo/fastapi) |
| `uvicorn[standard]` | ≥ 0.32 | BSD-3-Clause | [encode/uvicorn](https://github.com/encode/uvicorn) |
| `httpx` | ≥ 0.28 | BSD-3-Clause | [encode/httpx](https://github.com/encode/httpx) |

### Config, validation, logging

| Component | Version | License | Source |
|---|---|---|---|
| `pydantic` | ≥ 2.9 | MIT | [pydantic/pydantic](https://github.com/pydantic/pydantic) |
| `pydantic-settings` | ≥ 2.6 | MIT | [pydantic/pydantic-settings](https://github.com/pydantic/pydantic-settings) |
| `structlog` | ≥ 24.4 | Apache-2.0 / MIT (dual) | [hynek/structlog](https://github.com/hynek/structlog) |

### Observability

| Component | Version | License | Source |
|---|---|---|---|
| `prometheus-client` | ≥ 0.21 | Apache-2.0 | [prometheus/client_python](https://github.com/prometheus/client_python) |
| `opentelemetry-api` | ≥ 1.27 | Apache-2.0 | [open-telemetry/opentelemetry-python](https://github.com/open-telemetry/opentelemetry-python) |
| `opentelemetry-sdk` | ≥ 1.27 | Apache-2.0 | [open-telemetry/opentelemetry-python](https://github.com/open-telemetry/opentelemetry-python) |
| `opentelemetry-exporter-otlp-proto-grpc` | ≥ 1.27 | Apache-2.0 | [open-telemetry/opentelemetry-python](https://github.com/open-telemetry/opentelemetry-python) |
| `sentry-sdk` | ≥ 2.18 | MIT | [getsentry/sentry-python](https://github.com/getsentry/sentry-python) |

### Security

| Component | Version | License | Source |
|---|---|---|---|
| `bcrypt` | ≥ 4.2 | Apache-2.0 | [pyca/bcrypt](https://github.com/pyca/bcrypt) |
| `cryptography` | ≥ 43.0 | Apache-2.0 / BSD-3-Clause (dual) | [pyca/cryptography](https://github.com/pyca/cryptography) |

### Build and packaging

| Component | Version | License | Source |
|---|---|---|---|
| `hatchling` | (build backend) | MIT | [pypa/hatch](https://github.com/pypa/hatch) |
| `uv` | (dev) | Apache-2.0 / MIT (dual) | [astral-sh/uv](https://github.com/astral-sh/uv) |

### Dev / test toolchain

| Component | Version | License | Source |
|---|---|---|---|
| `ruff` | ≥ 0.6 | MIT | [astral-sh/ruff](https://github.com/astral-sh/ruff) |
| `black` | ≥ 24.8 | MIT | [psf/black](https://github.com/psf/black) |
| `isort` | ≥ 5.13 | MIT | [PyCQA/isort](https://github.com/PyCQA/isort) |
| `mypy` | ≥ 1.11 | MIT | [python/mypy](https://github.com/python/mypy) |
| `pytest` | ≥ 8.3 | MIT | [pytest-dev/pytest](https://github.com/pytest-dev/pytest) |
| `pytest-asyncio` | ≥ 0.24 | Apache-2.0 | [pytest-dev/pytest-asyncio](https://github.com/pytest-dev/pytest-asyncio) |
| `pytest-cov` | ≥ 5.0 | MIT | [pytest-dev/pytest-cov](https://github.com/pytest-dev/pytest-cov) |
| `pre-commit` | ≥ 3.8 | MIT | [pre-commit/pre-commit](https://github.com/pre-commit/pre-commit) |
| `freezegun` | ≥ 1.5 | Apache-2.0 | [spulec/freezegun](https://github.com/spulec/freezegun) |
| `grpcio-tools` | ≥ 1.66 | Apache-2.0 | [grpc/grpc](https://github.com/grpc/grpc) |
| `pyyaml` | ≥ 6.0 | MIT | [yaml/pyyaml](https://github.com/yaml/pyyaml) |
| `pip-audit` | ≥ 2.7 | Apache-2.0 | [pypa/pip-audit](https://github.com/pypa/pip-audit) |
| `aiosqlite` | ≥ 0.20 | MIT | [omnilib/aiosqlite](https://github.com/omnilib/aiosqlite) |

### Infrastructure

| Component | Role | License |
|---|---|---|
| Docker / Docker Compose | local data plane | Apache-2.0 |
| Kubernetes | production orchestration | Apache-2.0 |
| Helm | chart packaging (`deploy/helm/`) | Apache-2.0 |
| Envoy | TLS termination + ring-hash routing | Apache-2.0 |
| Prometheus | metrics scrape (sidecar) | Apache-2.0 |
| Grafana | dashboards (sidecar) | AGPL-3.0 (sidecar only — never bundled into the runtime image) |

License attributions for the transitive set are surfaced in
[`NOTICE`](./NOTICE) and verified in CI.

---

## Configuration

Everything is configured by environment variables, parsed by
`pydantic-settings`. The full reference is generated from the
`Settings` model — see [`docs/11-configuration-reference.md`](./docs/11-configuration-reference.md)
and [`.env.example`](./.env.example). The most common ones:

| Variable | Default | Purpose |
|---|---|---|
| `EVEYS_OCPP_WS_HOST` / `_PORT` | `0.0.0.0:9000` | WebSocket server (chargers connect here) |
| `EVEYS_OCPP_GRPC_HOST` / `_PORT` | `0.0.0.0:50051` | Internal gRPC API |
| `EVEYS_OCPP_REST_HOST` / `_PORT` | `0.0.0.0:8080` | Backend-facing REST API |
| `EVEYS_OCPP_REST_INBOUND_TOKENS` | `""` | Comma-separated bearer tokens. Empty rejects everything. |
| `EVEYS_OCPP_REST_OPENAPI_ENABLED` | `false` | Enable Swagger UI / ReDoc / `openapi.json` on `/api/v1/`. Off in production by default. |
| `EVEYS_OCPP_DB_URL` | `postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp` | Postgres connection |
| `EVEYS_OCPP_REDIS_URL` | `redis://localhost:6379/0` | Redis (registry + bus + idempotency cache) |
| `EVEYS_OCPP_KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap |
| `EVEYS_OCPP_CLICKHOUSE_HOST` / `_PORT` | `localhost:9000` | ClickHouse (native protocol) |
| `EVEYS_OCPP_BACKEND_BASE_URL` | `""` | Empty ⇒ no backend wired; Authorize falls back per `BACKEND_AUTHORIZE_FALLBACK`. |
| `EVEYS_OCPP_BACKEND_TOKEN` | `""` | Bearer token sent on every backend call |
| `EVEYS_OCPP_BACKEND_AUTHORIZE_FALLBACK` | `reject` | `reject` or `accept_offline` |
| `EVEYS_OCPP_SENTRY_DSN` | `""` | Empty ⇒ Sentry inactive |
| `EVEYS_OCPP_LOG_LEVEL` | `INFO` | Standard log levels |
| `EVEYS_OCPP_LOG_JSON` | `true` | JSON output (production) vs. human-readable (dev) |

`make config-schema` prints `Settings` as JSON Schema for
Helm-values validators or operator UIs.

---

## Project layout

```
src/eveys_ocpp/
├── __main__.py            entry point — boots WS + gRPC + REST + Kafka producer
│                          inside one asyncio.TaskGroup
├── settings.py            pydantic-settings — env-driven config (source of truth
│                          for docs/11-configuration-reference.md and .env.example)
├── transport/
│   ├── ws_server.py       OCPP WebSocket server (charger-facing)
│   ├── grpc_server.py     platform-facing gRPC API + cross-pod bus dispatch
│   └── rest_server.py     platform-facing REST API (FastAPI)
├── connection.py          ChargePoint subclass — protocol-version dispatch
├── handlers/
│   ├── v16/               OCPP 1.6 handlers (isolated)
│   └── v201/              OCPP 2.0.1 handlers (isolated)
├── api/                   FastAPI routers — read + command endpoints
├── platform/              backend HTTP client + Authorize cache
├── persistence/           SQLAlchemy 2.0 async + Alembic migrations
├── clickhouse/            ingestor sidecar + DDL migrator
├── webhooks/              Kafka-tail dispatcher (HMAC-signed)
├── registry.py            Redis online-charger registry
├── bus.py                 Redis pub/sub cross-pod command bus
├── events.py              Kafka producer (versioned envelope, cp_id partition key)
└── observability.py       structlog + Prometheus + OpenTelemetry + Sentry

proto/                     versioned gRPC + Kafka-event protobuf contracts
deploy/                    Dockerfile, compose, Helm chart, Envoy config
tests/{unit,e2e,compose_smoke,smoke,mock_backend}/
docs/                      architecture, ADRs, runbooks, OpenAPI spec
```

Cross-importing between `handlers/v16/` and `handlers/v201/` is
forbidden — same rule the upstream `mobilityhouse/ocpp` library
enforces. Each protocol version is its own self-contained surface.

---

## Tests

Four-tier ladder, each gated separately in CI:

1. **Unit** (`tests/unit/`) — pure Python, ≥ 80 % coverage gate.
2. **Integration / e2e** (`tests/e2e/`) — real Postgres, Redis,
   Kafka, ClickHouse via service containers in CI, docker-compose
   locally. Includes a **two-pod dispatch test**
   (`tests/e2e/test_two_pod_dispatch.py`) that exercises the
   cross-pod command bus.
3. **Compose smoke** (`tests/compose_smoke/`) — the production-shaped
   container image actually boots, drives a full charger flow.
4. **K8s smoke** (planned) — Helm chart deploys cleanly into kind/k3d.

Run them locally with `make tests`, `make e2e`, `make compose-smoke`.
Full strategy in [`docs/10-testing-strategy.md`](./docs/10-testing-strategy.md).

---

## Documentation

`docs/` is the source of truth for everything beyond this README:

- [`00-overview.md`](./docs/00-overview.md) — the elevator pitch
- [`01-roadmap.md`](./docs/01-roadmap.md) — phased delivery plan
- [`05-architecture-decisions.md`](./docs/05-architecture-decisions.md) — ADR index (the *why* behind every load-bearing decision)
- [`07-local-dev-setup.md`](./docs/07-local-dev-setup.md) — local dev
- [`08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md) — OCPP test-case coverage matrix
- [`10-testing-strategy.md`](./docs/10-testing-strategy.md) — the four-tier test ladder
- [`11-configuration-reference.md`](./docs/11-configuration-reference.md) — every setting
- [`12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md) — onboarding hardware
- [`13-load-testing.md`](./docs/13-load-testing.md) — 10 k-charger swarm test
- [`14-slos.md`](./docs/14-slos.md) — SLOs and error budgets
- [`15-openapi.md`](./docs/15-openapi.md) — OpenAPI / Swagger UI guide
- [`16-dr-runbook.md`](./docs/16-dr-runbook.md) — disaster recovery
- [`17-sizing.md`](./docs/17-sizing.md) — capacity planning
- [`18-rollback-runbook.md`](./docs/18-rollback-runbook.md) — production rollback
- [`integration/`](./docs/integration/) — backend ↔ gateway contract
- [`adr/`](./docs/adr/) — architecture decision records (one per decision, append-only)

Sphinx build wired up under `docs/` (`make docs`).

---

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md):

- Branch naming, PR size guidelines, [Conventional Commits](https://www.conventionalcommits.org/).
- Local quality gate (`make tests`) — green before you push.
- Code of Conduct in [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

Issues and feature requests go to GitHub Issues. Search before
filing.

---

## Security

Security reports do **not** go through the public issue tracker. See
[`SECURITY.md`](./SECURITY.md) for the private reporting channel,
supported-version policy, scope, and turnaround targets. For
non-vulnerability security questions: [info@eveys.com](mailto:info@eveys.com).

---

## Support

- Product info: [eveys.com](https://eveys.com)
- General contact: [info@eveys.com](mailto:info@eveys.com)
- Bug reports / feature requests: GitHub Issues
- Security disclosures: see [`SECURITY.md`](./SECURITY.md)

---

## License

Licensed under the Apache License, Version 2.0. See [`LICENSE`](./LICENSE)
for the full text and [`NOTICE`](./NOTICE) for attribution.

```
Copyright 2026 Eveys Mobility

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```

For licensing inquiries: [info@eveys.com](mailto:info@eveys.com).

---

## Acknowledgements

This project would not exist without the work of the open-source
community. Special thanks to:

- **The Mobility House** for [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp),
  the OCPP 1.6 / 2.0.1 / 2.1 reference implementation this service
  builds on.
- **The Open Charge Alliance (OCA)** for authoring and stewarding the
  OCPP specifications.
- The maintainers of every project listed in the [Stack](#stack)
  section above. Their licenses and copyrights are preserved in
  [`NOTICE`](./NOTICE).
