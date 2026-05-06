# 03 — Coding Standards

> Python conventions for `eveys/ocpp`. These are **rules**, not suggestions — CI enforces most of them.

## Language and runtime

- **Python 3.13** is the target runtime. Type hints use 3.13 syntax.
- **`asyncio`** is the concurrency model. `uvloop` in production. **No threads, no multiprocessing inside `ocpp-gw`** (workers off the loop are OK via `loop.run_in_executor`).
- **One process per pod.** Don't spawn child processes from the main service.

## Tooling — required, in this order

| Tool | Purpose | Config | CI gate |
|---|---|---|---|
| **`uv`** (or Poetry) | Dependency management, lockfile | `pyproject.toml` | `make install` succeeds |
| **`ruff`** | Linter (replaces flake8 + pyupgrade + pep8-naming) | `[tool.ruff]` in `pyproject.toml` | `ruff check .` zero errors |
| **`black`** | Formatter | line-length **100**, target-version `py313` | `black --check .` clean |
| **`isort`** | Import sorter | `profile = "black"`, `line_length = 100` | `isort --check-only` clean |
| **`mypy`** | Static type checker, **strict mode on `src/`** | `[tool.mypy]` in `pyproject.toml` | `mypy src` zero errors |
| **`pytest`** + **`pytest-asyncio`** + **`pytest-cov`** | Tests + coverage | `[tool.pytest.ini_options]` | All tests pass; coverage ≥ 80% |
| **`pre-commit`** | Run all the above on `git commit` | `.pre-commit-config.yaml` | (Local; CI re-runs) |

> Why `ruff` and not flake8? It's ~50× faster, replaces 4 separate tools, and we're starting fresh.
> Why line-length **100** (not 88 like `mobilityhouse/ocpp` itself)? Modern monitors, long type hints, gRPC fields. Stay consistent across our repo.

---

## Project layout

```
eveys/ocpp/
├── pyproject.toml
├── Makefile
├── README.md
├── docs/                 # this directory — markdown source + Sphinx build (conf.py, Makefile, requirements.txt)
├── proto/                # gRPC + event protobuf definitions (versioned)
│   ├── ocpp_gw/v1/
│   └── events/v1/
├── src/
│   └── eveys_ocpp/
│       ├── __init__.py
│       ├── __main__.py           # entry point: python -m eveys_ocpp
│       ├── settings.py           # pydantic-settings
│       ├── observability.py      # structlog, prometheus, otel setup
│       ├── transport/
│       │   ├── ws_server.py      # websockets.serve, upgrade handler
│       │   └── grpc_server.py    # gRPC API
│       ├── connection.py         # ChargePoint subclass
│       ├── handlers/
│       │   ├── v16/              # OCPP 1.6 handlers
│       │   └── v201/             # OCPP 2.0.1 handlers
│       ├── commands/
│       │   ├── v16.py            # gRPC → OCPP Call senders
│       │   └── v201.py
│       ├── platform/             # gRPC clients to auth/session/device
│       │   ├── auth.py
│       │   ├── session.py
│       │   └── device.py
│       ├── persistence/
│       │   ├── db.py             # asyncpg pool / SQLAlchemy engine
│       │   ├── models.py         # SQLAlchemy ORM
│       │   └── repositories.py   # query functions
│       ├── registry.py           # Redis online registry
│       ├── bus.py                # Redis pub/sub (cross-pod commands)
│       ├── events.py             # Kafka producer
│       └── auth.py               # WS-edge auth (when not at LB)
├── tests/
│   ├── conftest.py
│   ├── unit/
│   ├── integration/             # docker-compose Postgres+Redis+Kafka
│   ├── e2e/                     # full charger simulator
│   └── load/                    # locust / k6 scripts
├── deploy/
│   ├── Dockerfile
│   ├── helm/                    # Helm chart for k8s
│   └── envoy/                   # Envoy config templates
└── .github/
    └── workflows/             # GitHub Actions workflows (quality, tests, e2e, compose-smoke, docs)
```

**Rules**:
- All importable code lives under `src/eveys_ocpp/`. No top-level `eveys_ocpp.py` next to `pyproject.toml`.
- Per-protocol-version code is **strictly isolated** in `handlers/v16/`, `handlers/v201/`, `commands/v16.py`, `commands/v201.py`. **Never cross-import** — same rule as the upstream `mobilityhouse/ocpp` library.
- `proto/` is the contract surface. Treat it like a public API.

---

## Naming

| Element | Convention | Example |
|---|---|---|
| Module | `lower_snake_case` | `ws_server.py` |
| Package | `lower_snake_case` | `eveys_ocpp` |
| Class | `PascalCase` | `ChargePoint`, `BootNotificationHandler` |
| Function / method | `lower_snake_case` | `send_remote_start` |
| Constant | `UPPER_SNAKE_CASE` | `DEFAULT_HEARTBEAT_INTERVAL` |
| Private | leading `_` (single) | `_validate_payload` |
| Type alias | `PascalCase` | `ChargePointId = NewType("ChargePointId", str)` |
| Test | `test_<unit>_<scenario>` | `test_boot_notification_accepts_known_vendor` |
| OCPP message ID variable | `message_id` (always) | not `msg_id`, not `messageId` |
| Charger ID variable | `cp_id` (always) | not `chargerId`, not `charge_point_id` |

---

## Type hints

- **Mandatory** on all `src/` code (mypy strict).
- Use `from __future__ import annotations` everywhere — no quoted strings.
- Prefer `NewType` for IDs you don't want mixed up: `ChargePointId`, `TransactionId`, `IdTag`.
- **No `Any`** unless you also add a `# pragma: no cover` AND a comment explaining why.
- Tests can be untyped or loosely typed.

---

## Error handling

- **One exception base class per layer**: `OcppGatewayError` → `TransportError`, `ProtocolError`, `PersistenceError`, `PlatformError`. Subclass for specific cases.
- **No bare `except:`**. Always name the exception.
- **Don't swallow exceptions silently** — log them with context (`cp_id`, `message_id`).
- **Crash early on programmer errors** (assertion, bad config). Recover gracefully on operational errors (network blip, DB transient).
- **Never** let an unhandled exception kill the event loop. Top-level handlers wrap every charger task and log.

---

## Logging

- **`structlog`** with JSON renderer. No `print()`, no `logging.basicConfig()` outside `observability.py`.
- Every log line in the OCPP request path **must** carry: `cp_id`, `message_id` (when applicable), `action`, `direction` (in/out).
- Levels:
  - `DEBUG`: per-message wire log (off by default, on per-charger via env flag).
  - `INFO`: lifecycle events (connect, disconnect, transaction start/stop).
  - `WARNING`: recoverable anomalies (validation failure, retry).
  - `ERROR`: failures requiring attention.
  - `CRITICAL`: fleet-impacting events.
- Don't log payloads at `INFO`. Payloads are `DEBUG` only and are stored to `ocpp_messages` audit table.

---

## Async patterns

- **Every I/O is async.** No sync DB drivers, no `requests`, no blocking file I/O on the loop.
- Use **`asyncio.timeout()`** (3.11+) for time-bound operations. Default timeout for any external call: 5 seconds.
- **Never `await` while holding a lock unless you mean it.**
- **Don't use `asyncio.run()` inside library code** — only in `__main__.py`.
- **Connection pools are shared** (Postgres, Redis, Kafka). One per process, initialized at startup, closed on shutdown.

---

## Database

- **Migrations are mandatory** for every schema change. Alembic, not raw SQL.
- **No ORM in hot paths.** Use `asyncpg` directly for `MeterValues` ingest, `Heartbeat` updates. SQLAlchemy 2.0 async is fine for admin/CRUD.
- **No N+1 queries.** Use joins or batch loads.
- **Transactions are explicit.** No implicit commit-on-disconnect.
- **Idempotency keys** on writes that may be retried (transactions, command dispatches).

---

## Testing

| Test type | Location | What it tests | Speed target |
|---|---|---|---|
| Unit | `tests/unit/` | Pure functions, single class | < 1s total |
| Integration | `tests/integration/` | With Postgres + Redis + Kafka via testcontainers | < 30s total |
| E2E | `tests/e2e/` | Full simulator → service → DB | < 5min total |
| Load | `tests/load/` | 10k+ chargers (run on demand, not in MR CI) | runs in nightly job |

- **Coverage ≥ 80%** on `src/eveys_ocpp/`. Hard CI gate.
- **Every handler ships with a unit test.** MRs without tests are rejected.
- **Integration tests use real services** (testcontainers), not mocks. Mocks are for unit tests only.
- **No test depends on the network.** No real OCA OCTT, no real charger vendors. Those go in nightly OCTT runs.
- Test data is built with **factories**, not fixtures-frozen JSON.
- **No silent skips in CI.** A test that silently skips when its dependency
  (Redis, Postgres, schema, etc.) is missing produces a green-but-empty
  pipeline — false-green is a P1 risk. Every reachability check that gates
  test execution must honor the `E2E_REQUIRE=1` env var: skip on dev
  laptops (unset), `pytest.fail()` at collection in CI (set). Both the
  `tests` and `e2e` workflows in `.github/workflows/` set `E2E_REQUIRE=1`.
  Reference implementation: `tests/unit/test_bus.py` and
  `tests/e2e/test_two_pod_dispatch.py`.

---

## OCPP-specific rules

These are domain-critical. Violating them is a P1 incident waiting to happen.

1. **Never cross-import between `ocpp.v16` and `ocpp.v201`.** Same rule as the upstream library. Mixing them silently produces invalid messages.
2. **JSON Schemas in `mobilityhouse/ocpp` are authoritative.** Don't edit dataclasses without consulting the schema. Don't disable validation under load — fix the underlying issue.
3. **Always validate `cp_id` at every layer.** URL path, authenticated identity, and the message body must agree.
4. **Idempotent inbound `BootNotification` and `StopTransaction`.** A retry must not produce duplicate downstream effects.
5. **`MeterValues` go to Kafka, never to Postgres.** Use the time-series store.
6. **Message ordering is preserved per charger.** Use `cp_id` as the Kafka partition key. Don't fan messages from the same CP across partitions.
7. **Sanity-check meter values.** A charger reporting 100MWh in one transaction is a bug or attack — quarantine, don't bill.
8. **OCPP timestamps from chargers are untrusted.** Charger clocks drift. Always also record the server-receive timestamp.

---

## Git hygiene

- **Branch off `main`.** No long-lived feature branches.
- **Conventional Commits**: `feat:`, `fix:`, `chore:`, `docs:`, `refactor:`, `test:`, `perf:`, `ci:`.
  - Example: `feat(handlers): implement BootNotification (E1-5)`
  - Reference task IDs from `02-tasks.md`.
- **MRs are small.** Target < 400 lines of diff. Larger MRs need a checklist explaining why.
- **Squash on merge.** Linear history.
- **No force-push to shared branches.** Local rebase is fine.

---

## Documentation in code

- **Module docstrings** required on every module in `src/`. One sentence is fine.
- **Class docstrings** required on every public class. Explain the *why*, not just the *what*.
- **Function docstrings** required on public functions where the behavior isn't obvious from the type signature.
- Use **Google-style docstrings** (parsed by Sphinx if/when we add it).
- **Never write a comment that just restates the code.** Explain *why*, not *what*.

---

## Performance ground rules

- **Profile before optimizing.** `py-spy` is the default tool. Optimize on data, not vibes.
- **Avoid premature concurrency.** A simple sequential `await` is often faster than `asyncio.gather()` for two operations.
- **Bound everything.** Queues have max sizes. Connection pools have max sizes. HTTP clients have timeouts.
- **The hot paths are**: WS receive loop, JSON-Schema validation, Kafka publish. Profile these quarterly.

---

## What's explicitly forbidden

- ❌ `eval()`, `exec()`, dynamic imports based on user input.
- ❌ Pickle for IPC (use protobuf / JSON / msgpack).
- ❌ Logging the full `payload` of every OCPP message at `INFO`. (DEBUG only.)
- ❌ Sleeping in async code (`time.sleep`). Use `asyncio.sleep`.
- ❌ Disabling JSON-Schema validation in production code path.
- ❌ Cross-importing between `handlers/v16/` and `handlers/v201/`.
- ❌ Storing meter values, heartbeats, or status notifications in Postgres long-term.
- ❌ Adding a new top-level dependency without a justification in MR description.
- ❌ Bypassing pre-commit / CI hooks (`--no-verify`).

---

## When the rules are wrong

These are guardrails, not laws. If a rule causes more harm than good in a specific case, **open an ADR** documenting the exception. Don't bypass silently.
