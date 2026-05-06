# eveys/ocpp

OCPP gateway service for the **Eveys** EV-charging platform.

It owns every charger's WebSocket connection and gives the rest of the
platform a stable surface to talk to: gRPC + REST for commands, Kafka
for events, Postgres for relational state, ClickHouse for time-series.

Supports **OCPP 1.6** (production) and is built on the
[`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) library
with Python 3.13, asyncio, and uvloop.

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

(The host port is `19000` because the compose stack remaps the
container's `9000` to dodge ClickHouse's native protocol on the host.
Running outside compose, the WS port is `9000`.)

Hit the gateway's REST API:

```bash
curl -H "Authorization: Bearer dev-token" \
     http://localhost:8080/api/v1/charge-points
```

(Set `EVEYS_OCPP_REST_INBOUND_TOKENS=dev-token` first; see the
configuration section below.)

---

## Stack

- **Python 3.13** + asyncio + uvloop
- **websockets** for the OCPP transport
- **grpclib** for the platform-facing gRPC API
- **FastAPI** + uvicorn for the platform-facing REST API
- **httpx** for the outbound HTTP client to the backend
- **PostgreSQL** for relational state (chargers, transactions,
  reservations, charging profiles, local-auth lists)
- **Redis** for the online-charger registry, the cross-pod command
  bus, the Authorize cache, and the inbound idempotency cache
- **Kafka** as the event firehose
- **ClickHouse** as the time-series store (MeterValues, status
  history, boots, transaction starts) — fed via a Kafka consumer
  sidecar
- **Kubernetes** for orchestration in production

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
`make config-schema` to print the current schema as JSON.

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

Run them locally with `make tests`, `make e2e`, `make compose-smoke`.

---

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the workflow. Security
reports go to [`SECURITY.md`](./SECURITY.md). Conduct expectations are
in [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).

---

## License

Proprietary — Eveys Mobility. See [`LICENSE`](./LICENSE).

For licensing inquiries: mostafa21tr@gmail.com
