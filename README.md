<p align="center">
  <a href="https://eveys.com">
    <picture>
      <source media="(prefers-color-scheme: dark)" srcset="docs/_static/eveys-white.svg">
      <img src="docs/_static/eveys-white.svg" alt="Eveys" width="220">
    </picture>
  </a>
</p>

<h1 align="center">eveys/ocpp</h1>

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
  <a href="https://github.com/eveys-mobility/OCPP/stargazers"><img alt="Stars" src="https://img.shields.io/github/stars/eveys-mobility/OCPP?style=social"></a>
</p>

---

OCPP gateway service for the **Eveys** EV-charging platform.

It owns every charger's WebSocket connection and gives the rest of the
platform a stable surface to talk to: gRPC + REST for commands, Kafka
for events, Postgres for relational state, ClickHouse for time-series.

Supports **OCPP 1.6** in production and is built on the
[`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) library
with Python 3.13, asyncio, and uvloop.

- Website: [eveys.com](https://eveys.com)
- Contact: [info@eveys.com](mailto:info@eveys.com)
- License: [Apache-2.0](./LICENSE)

---

## Table of contents

- [What it does](#what-it-does)
- [Quick start](#quick-start)
- [Stack](#stack)
- [Configuration](#configuration)
- [Running it](#running-it)
- [Project layout](#project-layout)
- [Tests](#tests)
- [Documentation](#documentation)
- [Contributing](#contributing)
- [Security](#security)
- [Support](#support)
- [License](#license)
- [Acknowledgements](#acknowledgements)

---

## What it does

| Direction | Surface | Used for |
|---|---|---|
| Charger → Gateway | WebSocket on `:9000`, subprotocol `ocpp1.6` | Every OCPP CALL: BootNotification, Authorize, StartTransaction, MeterValues, StopTransaction, StatusNotification, etc. |
| Gateway → Backend | HTTP (`httpx`) | Authorize, session open/close, charger registration |
| Backend → Gateway | REST on `:8080` (`/api/v1/*`) | Read charger / transaction state, dispatch OCPP commands (RemoteStart, Reset, ReserveNow, …) |
| Gateway ↔ Gateway | gRPC on `:50051` + Redis pub/sub | Cross-pod command routing for chargers connected to other pods |
| Gateway → Bus | Kafka (`cp.boot`, `cp.status`, `cp.meter`, `tx.started`) | Event firehose → ClickHouse, BFFs, analytics |

The service runs all four transports (WS, gRPC, REST, Kafka producer)
in one process, one event loop, via `asyncio.TaskGroup`. See
`src/eveys_ocpp/__main__.py`.

---

## Quick start

Requirements:

- Python **3.13** or newer
- [`uv`](https://github.com/astral-sh/uv) for dependency management
- Docker + Docker Compose for the local data plane
- `make`

```bash
make install        # create .venv via uv, install deps, generate proto stubs
make tests          # full pre-commit gate (ruff, mypy --strict, pytest, ≥80% coverage)
make compose-up     # local Postgres + Redis + Kafka + ClickHouse + service container
make e2e            # full e2e: compose-up → alembic upgrade → e2e tests → compose-down
```

Connect a real charger (after `make compose-up`):

```
ws://<host>:19000/<cp_id>     # subprotocol: ocpp1.6
```

The host port is `19000` because the compose stack remaps the
container's `9000` to dodge ClickHouse's native protocol on the host.
Running outside compose, the WS port is `9000`.

Hit the gateway's REST API:

```bash
curl -H "Authorization: Bearer dev-token" \
     http://localhost:8080/api/v1/charge-points
```

Set `EVEYS_OCPP_REST_INBOUND_TOKENS=dev-token` first; see the
[Configuration](#configuration) section below.

---

## Stack

Everything below is what the service depends on at runtime, with the
pinned minor floor and the upstream license. All of them are
OSI-approved permissive or weak-copyleft; nothing here pulls
GPL/AGPL into the runtime.

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
| `redis` (python client) | ≥ 5.2 | MIT | [redis/redis-py](https://github.com/redis/redis-py) |
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
| Envoy | TLS termination + edge routing in front of the gateway | Apache-2.0 |

The authoritative version pins live in
[`pyproject.toml`](./pyproject.toml). License attributions for the
transitive set are surfaced in [`NOTICE`](./NOTICE) and verified in CI.

---

## Configuration

The service is configured entirely through environment variables,
parsed by `pydantic-settings`. Key knobs:

| Variable | Default | Purpose |
|---|---|---|
| `EVEYS_OCPP_WS_HOST` / `EVEYS_OCPP_WS_PORT` | `0.0.0.0:9000` | WebSocket server (chargers connect here) |
| `EVEYS_OCPP_GRPC_HOST` / `EVEYS_OCPP_GRPC_PORT` | `0.0.0.0:50051` | Internal gRPC API |
| `EVEYS_OCPP_REST_HOST` / `EVEYS_OCPP_REST_PORT` | `0.0.0.0:8080` | Backend-facing REST API |
| `EVEYS_OCPP_REST_INBOUND_TOKENS` | `""` | Comma-separated bearer tokens; empty rejects everything (production-safe default) |
| `EVEYS_OCPP_DB_URL` | `postgresql+asyncpg://eveys:eveys@localhost:5432/eveys_ocpp` | Postgres connection |
| `EVEYS_OCPP_REDIS_URL` | `redis://localhost:6379/0` | Redis connection |
| `EVEYS_OCPP_KAFKA_BROKERS` | `localhost:9092` | Kafka bootstrap |
| `EVEYS_OCPP_CLICKHOUSE_HOST` / `_PORT` | `localhost:9000` | ClickHouse (native protocol) |
| `EVEYS_OCPP_BACKEND_BASE_URL` | `""` | When empty, the gateway runs without a backend (Authorize falls back per `BACKEND_AUTHORIZE_FALLBACK`) |
| `EVEYS_OCPP_BACKEND_TOKEN` | `""` | Bearer token sent on every backend call |
| `EVEYS_OCPP_LOG_LEVEL` | `INFO` | Standard log levels |
| `EVEYS_OCPP_LOG_JSON` | `true` | JSON output (production) vs. human-readable (dev) |

The full list, with categories, ranges, secret-flag, and stability
guarantees, is generated from the `Settings` Pydantic model. Run
`make config-schema` to print the current schema as JSON. A copy of
the default surface is in [`.env.example`](./.env.example).

---

## Running it

### Local development

```bash
make compose-up                # data plane
make compose-wait              # block until everything healthy
.venv/bin/alembic upgrade head # Postgres schema
make ch-migrate                # ClickHouse schema
python -m eveys_ocpp           # run the service against the stack
```

The compose file under `deploy/compose/` brings up Postgres 16,
Redis 7, Kafka (KRaft), ClickHouse 24, and the gateway container.

### Container

```bash
make build-image               # builds eveys-ocpp:dev (~170 MB, distroless)
docker run -p 9000:9000 -p 50051:50051 -p 8080:8080 \
  -e EVEYS_OCPP_DB_URL=... \
  -e EVEYS_OCPP_REDIS_URL=... \
  -e EVEYS_OCPP_KAFKA_BROKERS=... \
  eveys-ocpp:dev
```

### Kubernetes

A Helm chart under `deploy/helm/` is the production deployment shape.
See `deploy/helm/eveys-ocpp/values.yaml` for the values surface.

---

## Project layout

```
src/eveys_ocpp/
├── __main__.py            # entry point — boots WS + gRPC + REST in one TaskGroup
├── settings.py            # pydantic-settings, env-driven config
├── transport/
│   ├── ws_server.py       # OCPP WebSocket server
│   ├── grpc_server.py     # platform-facing gRPC API
│   └── rest_server.py     # platform-facing REST API
├── connection.py          # ChargePoint subclass
├── handlers/
│   ├── v16/               # OCPP 1.6 handlers (isolated)
│   └── v201/              # OCPP 2.0.1 handlers (future, isolated)
├── api/                   # FastAPI routers (read + command endpoints)
├── platform/              # backend HTTP client + Authorize cache
├── persistence/           # SQLAlchemy 2.0 async, Alembic migrations
├── clickhouse/            # ingestor + DDL migrator
├── registry.py            # Redis online-charger registry
├── bus.py                 # Redis pub/sub cross-pod command bus
├── events.py              # Kafka producer
└── observability.py       # structlog + Prometheus + OpenTelemetry

proto/                     # versioned gRPC + Kafka-event protobuf contracts
deploy/                    # Dockerfile, compose, Helm chart, Envoy config
tests/{unit,e2e,compose_smoke,smoke,mock_backend}/
docs/                      # architecture, ADRs, runbooks, OpenAPI
```

Cross-importing between `handlers/v16/` and `handlers/v201/` is
forbidden — same rule the upstream `mobilityhouse/ocpp` library
enforces. Each protocol version is its own self-contained surface.

---

## Tests

Four-tier ladder:

1. **Unit** (`tests/unit/`) — pure Python, ≥ 80% coverage gate.
2. **Integration / e2e** (`tests/e2e/`) — real Postgres, Redis,
   Kafka, ClickHouse via service containers in CI, docker-compose
   locally.
3. **Compose smoke** (`tests/compose_smoke/`) — the production-shaped
   container image actually boots, drives a full charger flow.
4. **K8s smoke** (planned) — Helm chart deploys cleanly into kind/k3d.

Run them locally:

```bash
make tests              # unit + lint + types
make e2e                # full integration ladder
make compose-smoke      # container-image smoke
make audit              # pip-audit against the resolved dep set
```

CI mirrors the local invocations one-to-one.

---

## Documentation

The `docs/` tree is the source of truth for everything beyond this
README:

- [`docs/00-overview.md`](./docs/00-overview.md) — what and why
- [`docs/01-roadmap.md`](./docs/01-roadmap.md) — phased delivery plan
- [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md) — ADR index
- [`docs/07-local-dev-setup.md`](./docs/07-local-dev-setup.md) — local dev
- [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md) — OCPP test-case coverage
- [`docs/11-configuration-reference.md`](./docs/11-configuration-reference.md) — every setting
- [`docs/12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md) — onboarding a hardware charger
- [`docs/14-slos.md`](./docs/14-slos.md) — SLOs and error budgets
- [`docs/15-openapi.md`](./docs/15-openapi.md) — REST API spec
- [`docs/16-dr-runbook.md`](./docs/16-dr-runbook.md) and [`docs/18-rollback-runbook.md`](./docs/18-rollback-runbook.md) — operational runbooks

A Sphinx build is wired up under `docs/` for hosted docs.

---

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow:

- Branch naming, commit-message convention (Conventional Commits),
  PR size guidelines.
- Local quality gate (`make tests`) — green before you push.
- Code of Conduct in [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

Issues, feature requests, and questions go to GitHub Issues. Please
search before filing — duplicates get closed with a pointer to the
existing thread.

---

## Security

Security reports do **not** go through the public issue tracker. See
[`SECURITY.md`](./SECURITY.md) for the private reporting channel,
supported-version policy, scope, and turnaround targets. For
non-vulnerability security questions, [info@eveys.com](mailto:info@eveys.com)
is the right inbox.

---

## Support

- Product info: [eveys.com](https://eveys.com)
- General contact: [info@eveys.com](mailto:info@eveys.com)
- Bug reports / feature requests: GitHub Issues on this repo
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
