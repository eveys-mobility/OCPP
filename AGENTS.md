# AGENTS.md

Project conventions for `eveys/ocpp` — the dense operational reference any contributor should keep open.

> For the long-form contributor guide read [`docs/04-contributing.md`](./docs/04-contributing.md) and [`docs/03-coding-standards.md`](./docs/03-coding-standards.md). This file is the lossless cheat sheet.

---

## What `eveys/ocpp` is

`eveys/ocpp` is the OCPP gateway service of the **Eveys** EV-charging platform. It owns every charger's WebSocket connection and exposes a stable gRPC + Kafka contract to the rest of the platform.

Built on **Python 3.13 + asyncio + uvloop** with the [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) library as the protocol foundation.

For the full picture, read [`docs/00-overview.md`](./docs/00-overview.md). For the *why*, read [`docs/adr/`](./docs/adr/).

---

## Hard rules — never break

1. **Never cross-import between `ocpp.v16`, `ocpp.v201`, and `ocpp.v21`.** Each is a self-contained protocol surface; mixing them silently produces invalid messages on the wire. Same rule applies to our own `src/eveys_ocpp/handlers/v16/` vs `v201/`.
2. **JSON Schemas in `mobilityhouse/ocpp` are authoritative.** Don't hand-edit dataclasses without consulting the schema. Don't disable validation under load — fix the underlying bug.
3. **No opportunistic refactors.** Bug fix → fix the bug. Feature → add the feature. Don't reformat unrelated code, don't rename "while you're here," don't add type annotations as drive-by changes.
4. **No new top-level dependencies without a justification in the PR description.** Runtime deps are intentionally minimal.
5. **No cross-service imports.** `eveys/ocpp` does not `import` from other Eveys services. All inter-service communication is gRPC + Kafka events.
6. **No `--no-verify`.** Don't bypass pre-commit or CI hooks. If a hook fails, fix the underlying issue.
7. **Tests ship with code.** A PR with implementation but no tests is not done.

---

## Project layout

```
eveys/ocpp/
├── pyproject.toml             # Python deps, tool configs
├── Makefile                   # install / format / tests / docs
├── README.md
├── docs/                      # roadmap, tasks, standards, ADRs
├── proto/                     # gRPC + event protobufs
├── src/eveys_ocpp/            # all importable code
│   ├── transport/             # ws_server, grpc_server, rest_server
│   ├── connection.py          # ChargePoint subclass
│   ├── handlers/v16/          # OCPP 1.6 handlers (isolated!)
│   ├── handlers/v201/         # OCPP 2.0.1 handlers (isolated!)
│   ├── api/                   # FastAPI surface (read + command endpoints)
│   ├── platform/              # backend HTTP client + Authorize cache
│   ├── persistence/           # Postgres
│   ├── registry.py            # Redis online registry
│   ├── bus.py                 # Redis pub/sub (cross-pod commands)
│   ├── events.py              # Kafka producer
│   └── observability.py       # structlog + prometheus + otel
├── tests/{unit,integration,e2e,load}/
└── deploy/{Dockerfile,helm/,envoy/}
```

### Public API entry points

```python
from ocpp.v16 import ChargePoint, call, call_result   # protocol library (third-party)
from ocpp.routing import on                            # @on() decorator
from eveys_ocpp.connection import EveysChargePoint     # our subclass
```

---

## Toolchain

- **Python**: 3.13 (target). Type hints use 3.13 syntax.
- **Package manager**: `uv` (or Poetry) — see `pyproject.toml`.
- **Concurrency**: `asyncio` with `uvloop`. **No threads, no multiprocessing inside the service.**
- **Linter**: `ruff` (replaces flake8 + pyupgrade + others).
- **Formatter**: `black` (line-length **100**).
- **Imports**: `isort` (`profile = "black"`).
- **Type checker**: `mypy --strict` on `src/`.
- **Tests**: `pytest` + `pytest-asyncio` + `pytest-cov` (≥ 80% coverage).
- **Pre-commit**: runs all of the above on every commit.

---

## Required commands

Always use the Makefile targets — don't invoke underlying tools directly unless you have a reason:

| Command | What it does |
|---|---|
| `make install` | Install deps via `uv` / Poetry |
| `make format` | Apply `isort` then `black` |
| `make lint`   | Run `ruff check` |
| `make types`  | Run `mypy --strict src/` |
| `make tests`  | Full pre-commit gate: ruff, mypy, pytest with coverage |
| `make build-image` | Build the `eveys-ocpp:dev` container image |
| `make compose-up` / `compose-down` / `compose-status` | Bring the data plane up / down / inspect |
| `make e2e` | Tier-2 e2e: compose-up + alembic + ch-migrate + tests/e2e + compose-down |
| `make compose-smoke` | Tier-3 smoke: real built image in real compose stack (ADR-0024). Requires Docker. |
| `make config-export` | Regenerate `docs/11-configuration-reference.md` and `.env.example` from `Settings` (ADR-0025) |
| `make config-export-check` | Mirrors the E0-14 CI gate; fails if either generated file drifts from `Settings` |
| `make config-schema` | Print `Settings.model_json_schema()` for downstream surfaces (Helm validators, operator UIs) |
| `make docs` | Build the docs site (delegates to `docs/Makefile`) |

The documentation site is built from `docs/` using its own `Makefile` (Sphinx + MyST). See [`docs/README.md`](./docs/README.md#building-this-site). The repo-root `make docs` target delegates to it.

**Before declaring a task done, run `make tests`.** It is the same gate CI runs.

---

## Style summary

- **Formatter**: `black`, line-length **100**.
- **Imports**: `isort` `profile = "black"`.
- **Linter**: `ruff` (rule set in `pyproject.toml`).
- **Naming**: `lower_snake_case` for modules/functions/variables, `PascalCase` for classes, `UPPER_SNAKE_CASE` for constants. **Always use `cp_id` and `message_id`** as variable names — never `chargerId` or `msg_id`.
- **Type hints**: mandatory on all `src/` code. `from __future__ import annotations` everywhere. **No `Any`** without comment.
- **Docstrings**: required on public modules, classes, and non-obvious functions. Google style.
- **Comments**: explain *why*, not *what*. If removing a comment wouldn't confuse a reader, don't write it.

---

## Testing rules

- Coverage **≥ 80%** on `src/eveys_ocpp/`. Hard CI gate.
- **Every handler ships with a unit test.** Every gRPC method ships with an integration test.
- **Integration tests use real services** (testcontainers Postgres/Redis/Kafka), not mocks.
- **Mocks are for unit tests only.** Mocking integration tests defeats the purpose.
- **No test depends on the network.** No real OCA OCTT, no real charger vendors. Those go in nightly OCTT runs.
- Layout mirrors source: `tests/unit/handlers/v16/test_boot_notification.py` mirrors `src/eveys_ocpp/handlers/v16/boot_notification.py`.

---

## OCPP-specific rules

These are domain-critical. Violating them is a P1 incident waiting to happen.

1. **Never cross-import between `ocpp.v16` and `ocpp.v201`.** Restated because it's the most important rule. Same applies to our own `handlers/v16/` and `handlers/v201/`.
2. **Always validate `cp_id` at every layer.** URL path, authenticated identity, and message body must agree.
3. **Idempotent inbound `BootNotification` and `StopTransaction`.** Retries must not double-write.
4. **`MeterValues` go to Kafka, never to Postgres.** Use the time-series store (ClickHouse).
5. **Message ordering is preserved per charger.** Use `cp_id` as the Kafka partition key.
6. **Sanity-check meter values.** A charger reporting 100 MWh in one transaction is a bug or attack — quarantine, don't bill.
7. **OCPP timestamps from chargers are untrusted.** Always also record server-receive timestamp.
8. **Every handler PR updates [`docs/08-ocpp-conformance.md`](./docs/08-ocpp-conformance.md), keyed by Appendix C TC IDs** from the OCA OCPP 1.6 Certification Procedure (e.g., `TC_001` for Cold Boot Charge Point). New rows land at 🟡 (interim, non-certifiable). Promotion to ✅ requires the four-step process documented there: OCTT subset run, spec section reviewed, edge-case tests, declared deviations. **A 🟡 row may not be cited as "OCPP-conformant" in any external communication.** See also [`09-certification-readiness.md`](./docs/09-certification-readiness.md) for the program-level cert playbook.

---

## What not to do

- ❌ Cross-import between `ocpp.v16` / `v201` / `v21` (or our own subdirs).
- ❌ Edit JSON Schemas without an upstream-spec reason.
- ❌ Disable schema validation in production code path.
- ❌ Store `MeterValues`, `Heartbeats`, or `StatusNotifications` in Postgres long-term.
- ❌ Add runtime deps casually.
- ❌ Refactor code that isn't part of the task.
- ❌ Bypass `make tests` or pre-commit hooks.
- ❌ Use `print()`, `time.sleep()`, `requests` (sync HTTP), or other blocking calls in async code.
- ❌ Use `eval()`, `exec()`, dynamic imports based on user input.
- ❌ Cross-import between Eveys services. Use gRPC + Kafka.

---

## Git & PR conventions

- Branch off `main`. No long-lived feature branches.
- Conventional Commits: `feat(scope): subject (E<phase>-<seq>)`.
- PRs **< 400 lines** of diff (or include a checklist explaining why).
- Tests + docs **must** ship in the same PR as code changes.
- Squash on merge. Linear history.
- Reference task IDs from [`docs/02-tasks.md`](./docs/02-tasks.md) in PR titles.

---

## References

- [`docs/00-overview.md`](./docs/00-overview.md) — what this project is
- [`docs/01-roadmap.md`](./docs/01-roadmap.md) — phased plan
- [`docs/02-tasks.md`](./docs/02-tasks.md) — task IDs
- [`docs/03-coding-standards.md`](./docs/03-coding-standards.md) — full standards
- [`docs/04-contributing.md`](./docs/04-contributing.md) — workflow
- [`docs/05-architecture-decisions.md`](./docs/05-architecture-decisions.md) — ADRs
- [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) — protocol library
