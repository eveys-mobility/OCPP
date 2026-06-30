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

OCPP 1.6 / 2.0.1 CSMS gateway. Terminates charger WebSockets,
schema-validates and dispatches OCPP messages, and exposes a
backend-facing contract over REST, gRPC, Kafka, and webhooks.

Python 3.13 + asyncio + uvloop. Horizontally scaled out of the box.
Apache-2.0.

---

## Quickstart

Brings up the gateway + its data plane (Postgres, Redis, Kafka,
ClickHouse) on your laptop. Takes ~60 s on a fresh machine.

```bash
# Prereqs (macOS): Docker Desktop, Python 3.13, uv, make.
#   brew install python@3.13 uv
# `make doctor` lists anything missing.

git clone git@github.com:eveys-mobility/OCPP.git
cd OCPP
cp .env.example .env          # edit the placeholder tokens
make install                  # .venv + protoc + pre-commit
make compose-up               # data plane + gateway + migrations
```

| URL | What |
|---|---|
| `ws://localhost:19000/<cp_id>` | OCPP WebSocket (subprotocol `ocpp1.6`) |
| `http://localhost:8080/api/v1` | Gateway REST |
| `http://localhost:8080/api/v1/docs` | Swagger UI (set `EVEYS_OCPP_REST_OPENAPI_ENABLED=true`) |
| `http://localhost:9100/metrics` | Prometheus metrics |

Point an OCPP charger simulator at the WS URL and watch events flow.

## Update (rebuild + migrate + restart)

The one-shot updater rebuilds the gateway image, applies any new
Alembic migrations through the `migrate-postgres` service, and
recreates `ocpp` + `clickhouse-ingestor` in place. Never tears the
stack down; safe on a production VM.

```bash
make update                       # update the gateway
make update NO_PULL=1             # skip git pull
```

Scope is the **gateway only**. The
[**eveys-mobility/Console**](https://github.com/eveys-mobility/Console)
— the operator-facing system administration UI for this gateway
(sign-in protected, live snapshot+tail subscriptions, fleet view,
transaction detail, OCPP-config page, alerts) — is a separate
service with its own release cycle and its own updater. Run it
separately when you need to bump the Console:

```bash
cd ../eveys-console            # https://github.com/eveys-mobility/Console
sh scripts/updater.sh
```

See [`scripts/update.sh`](./scripts/update.sh) for the steps the
gateway updater walks through.

## Production safety

Set `EVEYS_ENV=production` (shell or `.env`). The Makefile then
refuses destructive targets that would stop the stack or wipe data:
`compose-down`, `compose-down-volumes`, `e2e`, `compose-smoke`,
`mock-backend`, `grafana-down`, `distclean`.

Override per call with `FORCE_PROD=1`:

```bash
EVEYS_ENV=production make compose-down                # refused
EVEYS_ENV=production FORCE_PROD=1 make compose-down   # runs
```

## Configuration

Two files at the repo root:

- **`.env.example`** — hand-curated quickstart template. The values
  most operators actually need to set. Copy to `.env`, fill in.
- **`.env.reference`** — generated from `src/eveys_ocpp/settings.py`
  via `make config-export`. Every available knob with documented
  defaults; ops reference, not a starter.

Field-by-field reference: [`docs/11-configuration-reference.md`](./docs/11-configuration-reference.md).

### Post-boot OCPP config matrix

After every Accepted `BootNotification`, the gateway pushes a set of
operator-tunable `ChangeConfiguration` calls
(`HeartbeatInterval`, `ConnectionTimeOut`, `MeterValuesSampledData`,
`StopTxnAlignedData`, `TransactionMessageAttempts`, …). The
measurand-list keys split on `charge_points.charger_type` (`ac` |
`dc` | NULL → AC) so DC sites get `SoC` and AC sites get
`Current.Export`. Every key is runtime-overridable through
`/api/v1/admin/config`; the
[Console](https://github.com/eveys-mobility/Console)'s **OCPP config**
page is the operator UI.

## Make targets — most-used

```bash
make compose-up         # bring up data plane + gateway + migrations
make compose-status     # container health
make migrate            # idempotent Postgres + ClickHouse migrations
make build-image        # rebuild eveys-ocpp:dev
make update             # rebuild + migrate + restart (gateway + console)
make get-token          # print a bearer token for Swagger UI
make grafana-up         # opt-in Grafana + Prometheus sidecar
make tests              # lint + mypy + pytest (fast inner loop)
```

`make help` prints the full list with one-line descriptions.

## Surfaces

| Surface | Bind | Direction |
|---|---|---|
| OCPP WebSocket | `:9000` (`:19000` under compose) | charger → gateway |
| REST | `:8080` (`/api/v1/...`) | backend / console → gateway |
| gRPC | `:50051` | backend → gateway |
| Kafka producer | `EVEYS_OCPP_KAFKA_BROKERS` | gateway → bus |
| Webhooks | gateway-initiated POST | gateway → backend |
| Metrics | `:9100/metrics` | Prometheus |

Backend integration contract: [`docs/integration/`](./docs/integration/).

## Documentation

| For | Start at |
|---|---|
| Conceptual overview | [`docs/00-overview.md`](./docs/00-overview.md) |
| Architecture decisions | [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md) and [`docs/adr/`](./docs/adr/) |
| OCPP conformance matrix | [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md) |
| Connecting a hardware charger | [`docs/12-connecting-real-charger.md`](./docs/12-connecting-real-charger.md) |
| SLOs / capacity / DR | [`docs/14-slos.md`](./docs/14-slos.md), [`docs/17-sizing.md`](./docs/17-sizing.md), [`docs/16-dr-runbook.md`](./docs/16-dr-runbook.md) |
| Rolling back a deploy | [`docs/18-rollback-runbook.md`](./docs/18-rollback-runbook.md) |
| Backend integration | [`docs/integration/`](./docs/integration/) |
| OpenAPI spec | [`docs/api/openapi.yaml`](./docs/api/openapi.yaml) |

Full Sphinx site: `make docs` (output under `docs/_build/html/`).

## Contributing

Branch naming, PR size, [Conventional Commits](https://www.conventionalcommits.org/),
and the local quality gate (`make tests`) live in
[`CONTRIBUTING.md`](./CONTRIBUTING.md). Code of Conduct:
[`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

## Security

Private disclosure channel and supported-version policy in
[`SECURITY.md`](./SECURITY.md). Non-vulnerability questions:
[info@eveys.com](mailto:info@eveys.com).

## License

Apache-2.0. See [`LICENSE`](./LICENSE) for the text and
[`NOTICE`](./NOTICE) for attribution.
