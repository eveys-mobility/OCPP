# ADR-0026: Gateway REST API — framework, auth, pagination, in-process ASGI

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-06 | Mostafa |

## Context

The gateway today serves only WebSocket (chargers, port 9000) and gRPC
(sibling services, port 50051). The backend has no synchronous read
path into gateway-known state. Per `docs/integration/02-gateway-rest-api.md`
(spec frozen alongside ADR-0023), the gateway must expose a REST
surface at `/api/v1/...` covering:

- **Read endpoints** (E3-7, this ADR): charge points, transactions,
  reservations, charging profiles (Postgres-backed); MeterValues and
  StatusNotification history (ClickHouse-backed).
- **Command endpoints** (E3-8, separate task): 19 OCPP CSMS-initiated
  commands, each a thin HTTP wrapper around an existing gRPC RPC.
- **Probe**: `/api/v1/health`.

ADR-0023 fixed the asymmetric envelope (raw response on this surface,
`{success, data, message}` on the backend-side surface). It did **not**
fix:

1. Which Python HTTP framework to use.
2. How inbound bearer tokens are validated (the existing `backend_token`
   is for outbound calls only).
3. How pagination cursors are shaped.
4. How the new HTTP server runs alongside the existing WS + gRPC
   servers.

This ADR records those four decisions so the implementer (E3-7) and the
follow-on commands MR (E3-8) share the same skeleton.

## Decision

### 1. Framework: FastAPI

Use **FastAPI ≥ 0.115** for the read REST surface, and (when E3-8 lands)
the command surface on the same app.

`tests/mock_backend/` (E3-10) already uses FastAPI. The team has the
muscle memory; the runtime venv adds two MB and one transitive
(starlette is already pulled by FastAPI's deps). Rolling our own
ASGI router for ~30 endpoints would burn more time than learning the
framework's quirks.

**Promotion**: FastAPI and `uvicorn[standard]` move from the dev/test
section of `pyproject.toml` to runtime. The mock backend becomes one
of several FastAPI users in this repo, no longer the only one.

### 2. Auth: per-token allowlist, secret, hard default

Bearer token in `Authorization` header. The gateway validates it
against an env-driven CSV allowlist:

```
EVEYS_OCPP_REST_INBOUND_TOKENS=token_for_backend,token_for_billing,...
```

Validation rules:

- Empty allowlist + `REST_AUTH_DISABLED=false` (the default) → reject
  every request with `401 UNAUTHORIZED`. Production safe-by-default.
- Empty allowlist + `REST_AUTH_DISABLED=true` → accept all requests,
  no header check. **Dev / laptop / unit-test convenience only**;
  the flag is loud (`stability=dev-only` in the metadata) and the
  start-up log line says `rest_auth.disabled=True` so a forgotten flip
  shows up in any log review.
- Non-empty allowlist → exact-match the bearer token against the list.

Tokens are flagged `secret: True` per ADR-0025. Phase 5 vault work
(E5-7) moves them to a SecretStr / vault fetch alongside the existing
`backend_token`.

Why a CSV allowlist and not a single token: the inbound surface has
multiple consumers (the eveys backend; potentially a billing back-fill
job; potentially an operator UI). Rotating one consumer without
flapping the others requires multiple valid tokens at the same time.
A CSV is the smallest shape that supports that.

### 3. Pagination: opaque keyset cursor

Cursor-based, not offset. Cursor is a base64-encoded JSON object
opaque to the client:

```python
# Postgres-backed endpoints (charge_points, transactions, ...):
{"id": <last_seen_pk>}

# ClickHouse-backed endpoints (meter_values, status_history):
{"ts": "<iso8601>", "id": "<last_event_id>"}
```

The server never reads or parses a cursor it didn't emit; a malformed
or non-decodable cursor → `400 BAD_REQUEST`. Documented as opaque in
the spec; the shape may change without backwards-compat consequences.

Why keyset over offset: stable under inserts (offset double-counts
or skips when rows shift), cheap on large tables (Postgres + the
existing PKs index). Why opaque: gives us room to evolve the shape
(e.g. include filter snapshot for invalidation) without breaking
clients.

`limit` is per-endpoint capped (default 100, max 500) via two new
settings to keep operators in control.

### 4. In-process ASGI alongside WS + gRPC

The REST server runs in the same Python process as the WebSocket and
gRPC servers, as a third task in the existing `asyncio.TaskGroup` in
`__main__.py`. Programmatic `uvicorn.Server` (not the CLI) so the
event loop, structured-logging context, and shutdown signals are
shared.

Bind: `0.0.0.0:8080` by default. Operators set `REST_ENABLED=false` to
skip booting the REST server entirely (e.g. for the
`clickhouse-ingestor` sidecar shape, which uses the same image but
never serves HTTP).

### Module layout

```
src/eveys_ocpp/transport/rest_server.py    # ASGI app factory + uvicorn driver
src/eveys_ocpp/api/__init__.py
src/eveys_ocpp/api/_auth.py                # bearer middleware
src/eveys_ocpp/api/_errors.py              # error envelope handlers
src/eveys_ocpp/api/_pagination.py          # cursor encode/decode
src/eveys_ocpp/api/health.py               # /api/v1/health
src/eveys_ocpp/api/charge_points.py        # /api/v1/charge-points*
src/eveys_ocpp/api/transactions.py         # /api/v1/charge-points/.../transactions, /transactions/{id}
src/eveys_ocpp/api/reservations.py         # E3-7 commit 3
src/eveys_ocpp/api/profiles.py             # E3-7 commit 3
src/eveys_ocpp/api/meter_values.py         # E3-7 commit 4 (ClickHouse-backed)
src/eveys_ocpp/api/status_history.py       # E3-7 commit 4 (ClickHouse-backed)
src/eveys_ocpp/api/commands.py             # E3-8 (one file or one-per-command, TBD)
```

Per-domain router files keep each readable; `transport/rest_server.py`
holds the ASGI machinery. Mirrors how `transport/grpc_server.py` is
monolithic per-transport but commands are domain-named within.

## Consequences

### Positive

- The gateway gets a synchronous read path for the backend without
  polling Kafka or running a SQL replica off Postgres.
- Same process = same `Settings`, `Registry`, `session_factory`,
  Redis client, ClickHouse client. No cross-process plumbing.
- Cursor pagination is opaque, so the implementation can evolve.
- Auth allowlist supports multi-consumer rotation.
- The framework choice (FastAPI) is the same one the mock backend
  uses, so `tests/mock_backend/` patterns apply directly to gateway
  routes.

### Negative

- FastAPI + uvicorn promotes ~four runtime deps (fastapi, starlette,
  uvicorn, h11). Acceptable; the WebSocket library already pulls
  most of them.
- An in-process REST server adds an attack surface to the gateway
  pod (the existing WS port faced chargers; the new REST port faces
  the backend). Mitigation: separate token allowlist + production
  network policy that restricts who can reach `:8080`.
- A misbehaving REST request handler can block the asyncio event loop
  and freeze the WS + gRPC servers. We rely on the FastAPI/asyncio
  contract that handlers are coroutines; structured logging + the
  Tier-3 compose smoke (ADR-0024) catch any handler that calls
  blocking sync code.
- The REST surface introduces a new versioning contract (`/api/v1/`).
  Any breaking change requires `/v2/` and migration coordination,
  same shape as the proto-breaking gate (ADR-0018).

### Rejected alternatives

- **Starlette directly without FastAPI.** Saves the FastAPI deps but
  we lose Pydantic body validation, OpenAPI generation, and dependency-
  injection ergonomics. The marginal binary size win isn't worth it.
- **A separate sidecar process.** Would isolate the REST handler from
  the WS/gRPC event loop's failure modes but doubles the pod footprint
  and forks the structured-logging context.
- **Offset pagination.** Simpler cursors but unstable on large
  growing tables. Keyset costs one tiny helper.
- **Public OpenAPI / docs UI.** Disabled by default — the contract
  is in `docs/integration/`, not at a discoverable URL on the
  gateway. We don't want the gateway to publish a self-describing
  schema to anyone who can curl it. **Amended 2026-05-07** (see below).

## Amendment 2026-05-07 — opt-in OpenAPI / Swagger UI

The blanket "OpenAPI is disabled" decision above turned out to be
slightly too strict in practice. Operators wanted a clickable Swagger
UI for dev / staging without rebuilding the binary, and the backend
team wanted a Postman-importable spec. The spirit of the original
decision (the gateway must not self-publish to anonymous callers)
is preserved with a narrower constraint:

- New setting `rest_openapi_enabled: bool = False` (per ADR-0025
  metadata). When **False** (default), the original behaviour stands:
  `docs_url=None`, `redoc_url=None`, `openapi_url=None`. Production
  deploys keep the toggle off.
- When **True**, FastAPI mounts `/api/v1/openapi.json` (the spec),
  `/api/v1/docs` (Swagger UI), and `/api/v1/redoc` (ReDoc). All three
  are subject to the same bearer-token auth as the rest of
  `/api/v1/*`; only token-bearers can read the spec. A boot-time
  WARNING log (`rest_openapi.enabled`) makes a forgotten flip
  greppable.
- The canonical spec for sharing lives at `docs/api/openapi.{json,yaml}`,
  regenerated via `make openapi-export`. Its drift is gated in CI by
  `make openapi-export-check`. The committed file is the artifact for
  Postman, external Swagger UIs, contract reviews, etc.; the runtime
  toggle is the dev-time clickable equivalent.
- Routes opt into rich OpenAPI schemas via `responses=` (and
  `openapi_extra={"requestBody": ...}` for POST bodies) rather than
  `response_model=`, because most routes return plain dicts and we
  don't want to risk runtime validation drift on a production surface.
  The trade-off (schemas can drift from real responses if not
  maintained) is mitigated by the snapshot test
  `tests/unit/api/test_openapi.py`.

This amendment doesn't widen what the gateway exposes by default; it
only adds an opt-in. The threat model is unchanged for production:
operators must explicitly turn the toggle on, and even then auth
gates the schema.

## References

- [`docs/integration/02-gateway-rest-api.md`](../integration/02-gateway-rest-api.md) — frozen contract.
- [`docs/15-openapi.md`](../15-openapi.md) — operator-facing how-to (E3-7 OpenAPI add-on).
- [ADR-0023](./0023-backend-rest-integration.md) — chose the asymmetric envelope.
- [ADR-0025](./0025-generated-config-reference.md) — every new Settings field needs full metadata.
- E3-7 (this MR), E3-8 (commands), E3-9 (webhooks) in `docs/02-tasks.md`.
