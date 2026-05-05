# eveys/ocpp

> **OCPP gateway service** for the **Eveys** EV-charging platform.

`eveys/ocpp` is a standalone, horizontally scalable Python service that owns every charger's WebSocket connection and exposes a stable internal API (gRPC + Kafka events) for the rest of the platform.

This repository has **closed Phase 2 — Full OCPP 1.6 Core** as of 2026-05-04. Phase 1 closed earlier: the WS server, the seven Phase-1 handlers, the Postgres schema, and the local docker-compose stack all run. Phase 2 landed the v1 protos, the gRPC server, the seven Phase-2 RPC bodies — `RemoteStart`, `RemoteStop`, `Reset`, `ChangeConfiguration`, `TriggerMessage`, `UnlockConnector`, `GetChargerStatus` — with **cross-pod routing** via Redis pub/sub (E2-10, see ADR-0016), the Redis online registry (E2-9), the four-topic Kafka event firehose `cp.boot`/`cp.status`/`cp.meter`/`tx.started` (E2-8), the **idempotency cache** for `BootNotification`/`StopTransaction` replays (E2-11, see ADR-0017), and the **ClickHouse landing path** for that firehose (E2-13/E2-14, see ADR-0020) via a sidecar Kafka→CH consumer. The Kafka producer runs `acks=all` + idempotent-producer for durability on the billing-relevant topics (E2-7, see ADR-0019), and gRPC + Kafka-event backward-compat is machine-checked in CI (E2-12, see ADR-0018). Long-tail E2-1 has shipped seven more RPCs across three sub-MRs: **E2-1A** completed OCPP 1.6 **Core profile** with `DataTransfer` (in + out), `GetConfiguration`, `ClearCache`; **E2-1B** completed the **LocalAuthList profile** with `GetLocalListVersion`, `SendLocalList` and two new Postgres tables; **E2-1C** completed the **Reservations profile** with `ReserveNow`, `CancelReservation` and the `reservations` table (charger-side authority + gateway-side mirror per ADR-0021). The gRPC surface is now 14 RPCs. Remaining: Smart Charging (E2-1E, with its own ADR) and Diagnostics + Firmware (E2-1F). See [`docs/02-tasks.md`](./docs/02-tasks.md).

## Quick start

```bash
make install        # create .venv via uv + install runtime + dev deps + run protoc
make tests          # full pre-commit gate (ruff, mypy, pytest with coverage)
make format         # auto-format (isort + black)
make compose-up     # local Postgres + Redis + Kafka + ClickHouse + the service container
make e2e            # full e2e: compose-up → alembic upgrade → e2e tests → compose-down
```

The service exposes the WS endpoint on `:9000` (chargers connect here) and the gRPC endpoint on `:50051` (the rest of the platform calls in here). Both are started together by `python -m eveys_ocpp` via an `asyncio.TaskGroup` (see `src/eveys_ocpp/__main__.py`).

## Documentation

Start here: [`docs/`](./docs/)

| Doc | What |
|---|---|
| [`docs/00-overview.md`](./docs/00-overview.md) | What this service is, where it fits |
| [`docs/01-roadmap.md`](./docs/01-roadmap.md) | Phased plan to GA |
| [`docs/02-tasks.md`](./docs/02-tasks.md) | Concrete task breakdown with IDs (`E0-1`, `E1-3`, …) |
| [`docs/03-coding-standards.md`](./docs/03-coding-standards.md) | Python conventions |
| [`docs/04-contributing.md`](./docs/04-contributing.md) | Branches, MRs, AI-assisted development rules |
| [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md) | ADR index |
| [`docs/07-local-dev-setup.md`](./docs/07-local-dev-setup.md) | Local development setup (docker-compose + k3d/kind) |
| [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md) | OCPP conformance matrix (TC ID → handler → status) |
| [`docs/09-certification-readiness.md`](./docs/09-certification-readiness.md) | Certification readiness playbook (PICS, OCTT, lab, exit gate) |

For AI assistants (Claude Code, Cursor, Copilot, Aider): see [`AGENTS.md`](./AGENTS.md).

## Building the docs site

The docs are rendered as a static HTML site by Sphinx + MyST. From the repository root:

```bash
cd ocpp/docs
make install      # creates docs/.venv/ and installs Sphinx + extensions (first run only)
make html         # renders the site to docs/_build/html/
```

Open `docs/_build/html/README.html` in a browser to view the site.

To share a local build with a teammate on the same network (Wi-Fi / office LAN / VPN):

```bash
cd ocpp/docs
make install
make html
python3 -m http.server 8000 --bind 0.0.0.0 --directory _build/html
```

The site is then reachable at `http://<host-LAN-IP>:8000/`. Find the host's LAN IP with `ipconfig getifaddr en0` (macOS) or `hostname -I` (Linux). Stop the server with `Ctrl+C`.

To clean up build artifacts:

```bash
cd ocpp/docs
make clean        # removes docs/_build/ (rebuild is fast — venv kept)
make distclean    # removes docs/_build/ AND docs/.venv/ (next build re-creates the venv)
```

For full details — CI behavior, build configuration, troubleshooting — see [`docs/README.md`](./docs/README.md#building-this-site).

## Stack

- **Python 3.13 + asyncio + uvloop**
- **[`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp)** — official Python OCPP library (1.6 / 2.0.1 / 2.1)
- **`websockets`** for transport
- **gRPC** (`grpclib` async) for internal API
- **Postgres** (state) · **Redis** (registry, cache, pub/sub) · **Kafka** (event firehose)
- **ClickHouse** (time-series store for `MeterValues`, `StatusNotifications`, `BootNotifications`, `StartTransactions`; Heartbeats are absorbed by the Redis online registry per ADR-0020)
- **Kubernetes** for orchestration

See [ADR-0001](./docs/adr/0001-python-asyncio-stack.md), [ADR-0002](./docs/adr/0002-mobilityhouse-ocpp-library.md), and [ADR-0003](./docs/adr/0003-monorepo-layout.md) for *why*.

## Status

| Field | Value |
|---|---|
| Phase | **3 — Platform integration** (Phase 2 closed 2026-05-04; OCPP 1.6 Core ✅ · LocalAuthList ✅ · Reservations ✅; long-tail E2-1 remaining: Smart Charging, Diagnostics + Firmware) |
| Tech lead | TBD |
| License | Proprietary — Eveys |
