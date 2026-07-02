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
  <a href="./LICENSE"><img alt="License: Apache-2.0" src="https://img.shields.io/badge/License-Apache_2.0-blue.svg"></a>
  <a href="https://www.python.org/downloads/"><img alt="Python 3.13+" src="https://img.shields.io/badge/python-3.13%2B-blue"></a>
  <img alt="OCPP 1.6 / 2.0.1" src="https://img.shields.io/badge/OCPP-1.6%20%7C%202.0.1-blueviolet">
</p>

# eveys-mobility/ocpp

OCPP-J 1.6 CSMS gateway. Python 3.13 on `asyncio` + `uvloop`,
FastAPI for REST, `grpclib` for gRPC, Kafka for the event firehose,
HMAC webhooks for at-least-once push. Postgres for relational state,
ClickHouse for time-series.

---

## Quickstart

The compose stack brings up the gateway plus everything it needs
(Postgres, Redis, Kafka, ClickHouse) and applies the schema migrations
before declaring itself ready. On a fresh system it takes around a
minute.

```bash
# Prereqs: Python 3.13 and uv — install via your platform's package
# manager (apt/dnf/pacman/brew) or see https://docs.astral.sh/uv/getting-started/.
# `make doctor` lists anything else missing.
git clone git@github.com:eveys-mobility/OCPP.git
cd OCPP
cp .env.example .env                    # then edit the placeholder tokens
make install
make compose-up
```

When that returns, you have:

- An OCPP WebSocket on `ws://localhost:19000/<cp_id>` ready for a
  charger or simulator to connect.
- A REST surface on `http://localhost:8080/api/v1` for the backend.
- Prometheus metrics on `http://localhost:9100/metrics`.

Point an OCPP charger simulator at the WebSocket URL, and you'll see
the boot, status, and meter events flow through Kafka into ClickHouse
within seconds.

### Swagger UI and API authorization

- **Reach the Swagger UI** at `http://localhost:8080/api/v1/docs`.
  Off by default in production; enable it with
  `EVEYS_OCPP_REST_OPENAPI_ENABLED=true` (set in `.env` or in front of
  `make compose-up`).
- **Get a bearer token** with `make get-token` — reads
  `EVEYS_OCPP_REST_INBOUND_TOKENS` from your shell or `.env` and prints
  the first entry, pipe-friendly (e.g. `make get-token | pbcopy`).
- **Authorize** in Swagger by clicking the **Authorize** button and
  pasting the token. The same token is the `Authorization: Bearer …`
  value every other REST caller uses — backend, scripts, the Console.
- **Static spec** for sharing without a running gateway:
  `make openapi-export` writes `docs/api/openapi.{json,yaml}` from
  the live FastAPI app.

## Operating it

The Makefile groups the daily operations into a handful of verbs.
`make compose-up` is the canonical "make me a working stack" entry
point. `make compose-status` reports container health. `make migrate`
re-applies migrations idempotently after a schema change. `make
get-token` prints a bearer token for Swagger so you don't have to dig
through env files.

For a rolling update on a server, `make update` rebuilds the image,
runs the migrations against the live Postgres before the gateway
starts, recreates the gateway containers, and waits for `/api/v1/ready`
to come back green. It never tears the stack down, so it's safe to run
on a host that's serving live traffic. `NO_PULL=1` skips `git pull`.

Production hosts should mark themselves by setting `EVEYS_ENV=production`
— in the shell, or in `.env` (any leading whitespace is tolerated). On
a marked host the Makefile refuses anything destructive: stopping the
stack, wiping volumes, running the e2e suite, dropping the venv. Each
refusal prints the exact override (`FORCE_PROD=1`) for the rare
intentional case.

### Make targets

The verbs you'll actually reach for, grouped by when. `make help`
prints the live list.

**Setup**

| Target | What it does |
|---|---|
| `make doctor` | Check the system has the required tools. |
| `make install` | Create `.venv/`, install deps, regenerate proto stubs, install git hooks. |

**Stack**

| Target | What it does |
|---|---|
| `make compose-up` | Bring up Postgres + Redis + Kafka + ClickHouse + the gateway; apply all migrations. |
| `make compose-status` | Container health. |
| `make compose-wait` | Block until everything reports `healthy`. |
| `make compose-down` | Stop containers, keep data volumes. *(production-gated)* |
| `make compose-down-volumes` | Stop **and wipe** Postgres / Kafka / ClickHouse volumes. *(production-gated)* |
| `make build-image` | Build `eveys-ocpp:dev` from `deploy/Dockerfile`. |
| `make migrate` | Re-apply Postgres + ClickHouse migrations against the running stack. Idempotent. |
| `make get-token` | Print a bearer token from `.env` for Swagger Authorize. |
| `make grafana-up` / `make grafana-down` | Opt-in Grafana + Prometheus sidecar. |

**Code quality**

| Target | What it does |
|---|---|
| `make format` | `isort` + `black`. |
| `make lint` | `ruff check`. |
| `make types` | `mypy --strict`. |
| `make tests` | The local pre-commit gate: lint + types + pytest with the 80% coverage floor. |
| `make audit` | `pip-audit` against the resolved venv. |
| `make precommit` | Run every pre-commit hook against every file. |

**Test ladder**

| Target | What it does |
|---|---|
| `make tests` | Tier 1 — unit + lint + types. Fast inner loop. |
| `make e2e` | Tier 2 — compose-up, run `tests/e2e/`, compose-down. *(production-gated)* |
| `make compose-smoke` | Tier 3 — build the production image, run the compose-shaped smoke suite. *(production-gated)* |

**Deployment**

| Target | What it does |
|---|---|
| `make update` | One-shot rebuild → migrate → recreate `ocpp` + `clickhouse-ingestor` → poll `/api/v1/ready`. Never tears the stack down. `NO_PULL=1` skips `git pull`. |

**Docs and generated artifacts**

| Target | What it does |
|---|---|
| `make docs` / `make docs-clean` | Build / clean the Sphinx docs site. |
| `make config-export` | Regenerate `docs/11-configuration-reference.md` + `.env.reference` from `settings.py`. |
| `make openapi-export` | Regenerate `docs/api/openapi.{json,yaml}` from the live FastAPI app. |
| `make config-schema` | Print `Settings` as JSON Schema. |

**Cleanup**

| Target | What it does |
|---|---|
| `make clean` | Drop `.pytest_cache`, `.mypy_cache`, `__pycache__`, … . |
| `make distclean` | `clean` + drop `.venv/`. *(production-gated)* |

## Configuration

Configuration is environment-driven, with a hand-curated quickstart
file and a generated full reference next to it:

- `.env.example` is the human starter — only the values most operators
  actually need to set, with placeholders pointing at `openssl rand`
  for the secrets. Copy it to `.env` and fill the gaps.
- `.env.reference` is the generated catalogue of every knob the
  gateway supports, regenerated from `src/eveys_ocpp/settings.py` by
  `make config-export`. Treat it as documentation, not a starter.
- `docs/11-configuration-reference.md` is the same catalogue rendered
  as Markdown with descriptions and "what changes if you change
  this" notes.

A subset of settings are runtime-overridable — log level, rate-limit
toggles, the post-boot ChangeConfiguration matrix, webhook URLs — via
`PATCH /api/v1/admin/config`. The system administration console
([eveys-mobility/Console](https://github.com/eveys-mobility/Console))
is the operator UI for those.

## How it fits together

The gateway exposes five surfaces. Chargers connect inbound over
WebSocket; the backend talks to it over REST or gRPC, subscribes to
Kafka for the event firehose, or receives webhook deliveries for the
events that need at-least-once push. The gateway also calls back into
the backend on the hot path — `POST /sessions/open`, `/sessions/close`,
`Authorize` — over `httpx`, with a circuit breaker and configurable
fallback policies so a flaky backend can't take the WebSocket layer
down with it.

### Kafka topics

Every event the gateway publishes uses the same protobuf envelope
(`proto/events/v1/events.proto`), partitioned on `cp_id` so per-charger
ordering survives the broker. Defaults are overridable per topic via
`EVEYS_OCPP_KAFKA_TOPIC_*` env vars.

| Topic | When it fires |
|---|---|
| `cp.connected` | Charger accepted on the WS layer (post-handshake). |
| `cp.disconnected` | WS closed; carries the close reason. |
| `cp.offline_duration` | Reconnect after an outage; carries the gap length. |
| `cp.boot` | `BootNotification` accepted. |
| `cp.heartbeat` | Each accepted `Heartbeat`. |
| `cp.status` | Each `StatusNotification` (per-connector transitions). |
| `cp.meter` | Each `MeterValues` sample. |
| `cp.firmware_status` | `FirmwareStatusNotification` transitions. |
| `cp.diagnostics_status` | `DiagnosticsStatusNotification` transitions. |
| `cp.security_event` | OCPP 1.6 Security Whitepaper §4.6 events. |
| `cp.credential_rotated` | Basic-auth password set / removed via the credentials API. |
| `cp.csr_submitted` | Charger-initiated CSR awaiting operator approval. |
| `cp.ocpp_frames` | Full inbound + outbound frame audit (off by default — high volume). |
| `tx.started` | `StartTransaction` accepted. |
| `tx.stopped` | `StopTransaction` accepted; carries `consumed_wh`. |

The webhook dispatcher tails the same topics for the subset of events
configured for at-least-once HTTP push (`EVEYS_OCPP_WEBHOOK_ENABLE_*`).

The two repositories work as a pair:

- **eveys-mobility/ocpp** (this repo) — the protocol gateway. Owns
  the WebSocket connection to each charger, validates and dispatches
  OCPP messages, and publishes the canonical event stream.
- **[eveys-mobility/Console](https://github.com/eveys-mobility/Console)**
  — a sign-in protected operator UI for SREs and on-call engineers
  running this gateway. Live fleet view, transaction detail, alerts,
  runtime configuration. It's a consumer of the gateway; deploying or
  updating it is independent.

The backend contract for upstream integrators lives under
[`docs/integration/`](./docs/integration/).

## Going deeper

If you're trying to understand a particular slice of the codebase:

- [`docs/00-overview.md`](./docs/00-overview.md) — what the gateway
  owns, what it doesn't, and why the boundary is where it is.
- [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md)
  and [`docs/adr/`](./docs/adr/) — every significant decision in the
  codebase has an ADR explaining what was chosen, what was rejected,
  and why.
- [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md) —
  the per-OCPP-test-case conformance matrix.
- [`docs/12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md)
  — onboarding a hardware OCPP charger.
- [`docs/14-slos.md`](./docs/14-slos.md), [`docs/17-sizing.md`](./docs/17-sizing.md),
  and [`docs/16-dr-runbook.md`](./docs/16-dr-runbook.md) — the SLO
  commitments, capacity planning, and disaster-recovery drill kit.
- [`docs/api/openapi.yaml`](./docs/api/openapi.yaml) — the canonical
  OpenAPI 3.1 spec for the REST surface.

The full Sphinx site builds with `make docs`.

## Contributing, security, license

[`CONTRIBUTING.md`](./CONTRIBUTING.md) covers branch naming, PR size,
Conventional Commits, and the local quality gate. The Code of Conduct
is in [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

Security issues do not go through the public tracker — see
[`SECURITY.md`](./SECURITY.md) for the private channel, supported-version
policy, and our response targets. Non-vulnerability questions:
[info@eveys.com](mailto:info@eveys.com).

Released under the Apache License, Version 2.0 — see
[`LICENSE`](./LICENSE) and [`NOTICE`](./NOTICE).
