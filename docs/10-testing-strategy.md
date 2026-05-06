# Testing strategy

> **Why this doc exists.** A green CI pipeline previously coexisted with a
> deployable that crashed on `docker compose up`. [ADR-0024](./adr/0024-test-trust-ladder.md)
> records the decision; this doc is the operational guide. If you are
> adding a new test, deciding which tier to add it to, or wondering why
> a CI job is failing, start here.

The repository has **four** test tiers. Each tier owns a single bug
class. If a class of bug isn't owned by a tier, file an ADR — don't
shim a check into a tier built for something else.

## The ladder

```text
              ┌──────────────────────────────────────────────┐
   slowest ◄─ │  Tier 4 — Production-shape (k8s/Helm) [TBD]  │ ─► most realistic
              ├──────────────────────────────────────────────┤
              │  Tier 3 — Container compose-smoke            │
              ├──────────────────────────────────────────────┤
              │  Tier 2 — Integration with real services     │
              ├──────────────────────────────────────────────┤
   fastest ◄─ │  Tier 1 — Unit                               │ ─► least realistic
              └──────────────────────────────────────────────┘
```

| Tier | Path | CI job | Local make target | What it proves |
|---|---|---|---|---|
| 1 | `tests/unit/` | `tests` | `make tests` | Pure-Python logic. ≥ 80 % coverage gate. |
| 2 | `tests/e2e/` | `tests:e2e` | `make e2e` | Code talks to real Postgres / Redis / Kafka / ClickHouse. |
| 3 | `tests/compose_smoke/` | `tests:compose-smoke` | `make compose-smoke` | The **built container image** boots and stays up under the **production-shaped** `docker-compose.yml`. |
| 4 | `tests/k8s_smoke/` (TBD) | `tests:k8s-smoke` | `make k8s-smoke` | Helm chart deploys cleanly into kind/k3d. Lands with Phase 4. |

Plus: `tests/smoke/` (mock-backend standalone), `tests/mock_backend/`
(unit-level FastAPI tests). Those are smaller, ad-hoc smoke layers
attached to specific subsystems and not part of the ladder.

## Tier 1 — Unit

**Owns:** handler logic, repository SQL shape, settings parsing, cache
defensiveness, message serialisation.

**Forbids:** real network calls, real database calls, sleeps over a
few hundred ms, container spawning.

**Local:** `make tests` — runs ruff, mypy --strict, pytest with the
80 % coverage gate.

**CI:** `tests` job, runs on every push.

## Tier 2 — Integration with real services

**Owns:** the assertion that our Python code talks correctly to the
exact Postgres/Redis/Kafka/ClickHouse versions we pin. Schema
migrations apply cleanly. The Kafka → ClickHouse pipeline materialises
rows. Two-pod gRPC dispatch round-trips via the bus.

**Forbids:** assumptions about the **packaged container** working — Tier
2 still imports the gateway as a Python module and runs it as a
test-spawned subprocess. The Dockerfile and compose file are not
exercised here.

**Local:** `make e2e` — brings the data plane up via the dev compose
file, applies Alembic + ClickHouse migrations, runs `pytest tests/e2e`,
tears down.

**CI:** `e2e` workflow. Uses GitHub Actions `services:` containers
instead of the dev compose file (faster, simpler runner topology).
The image versions match `deploy/compose/docker-compose.yml`.

## Tier 3 — Container compose-smoke

**Owns:** "the binary boots." Specifically:

- `eveys-ocpp:dev` built from `deploy/Dockerfile` starts cleanly under
  the **production-shaped** `deploy/compose/docker-compose.yml` and
  stays running for at least 15 s with `restart_count == 0`.
- `clickhouse-ingestor` (same image, different `command:`) runs the
  ingestor module — not the gateway — and stays up.
- A real OCPP 1.6 charger driving Boot → Authorize → StartTransaction
  → MeterValues → StopTransaction over the host port produces the
  expected rows in Postgres and ClickHouse.

**Forbids:** unit-style assertions on internals. Tier 3 sees the
service through the same surface a customer's charger does — WS frames
in, DB rows / log lines out.

**Local:**

```bash
make compose-smoke
```

Steps run by the target:

1. `docker compose -f deploy/compose/docker-compose.yml down --volumes`
   (clean slate)
2. `docker compose ... up -d --build` (builds the image, starts the stack)
3. `make compose-wait` (block on healthchecks)
4. `alembic upgrade head` (Postgres schema)
5. `make ch-migrate` (ClickHouse schema)
6. `pytest tests/compose_smoke -v --no-cov`
7. `docker compose ... down --volumes` (tear down on success or fail)

**CI:** `compose-smoke` workflow. Uses the runner's preinstalled Docker
daemon directly — no DinD wrapping needed on GitHub Actions. Triggered
on default branch and on PRs that touch `deploy/`, `tests/compose_smoke/`,
or `pyproject.toml`. ~ 90 s.

### Why every tier exists, with bugs they catch

| Bug | Tier 1 | Tier 2 | Tier 3 |
|---|---|---|---|
| Authorize handler returns `Accepted` for a blocked id_tag | ✅ | ✅ | ✅ |
| Settings parsing rejects `BACKEND_AUTHORIZE_CACHE_TTL_SECONDS=foo` | ✅ | ✅ | ✅ |
| Alembic migration breaks against real Postgres | ❌ | ✅ | ✅ |
| Kafka producer config rejected by real broker | ❌ | ✅ | ✅ |
| **`EVEYS_OCPP_KAFKA_BROKERS` missing from compose `ocpp:` env** | ❌ | ❌ | ✅ |
| **`KAFKA_ADVERTISED_LISTENERS` only includes `localhost:9092`** | ❌ | ❌ | ✅ |
| **Compose `command:` silently swallowed by `ENTRYPOINT` shape** | ❌ | ❌ | ✅ |
| **`make compose-up` brings up containers that exit immediately** | ❌ | ❌ | ✅ |
| Helm template renders incorrect values | ❌ | ❌ | ❌ (Tier 4 future) |

The bold rows are real bugs that shipped green and motivated this layer.

## Tier 4 — Production-shape (future)

Lands with Phase 4 (`deploy/k8s/`). Brings up the Helm chart in a
kind/k3d cluster, runs the same charger flow against the cluster
ingress. Out of scope until then.

## How to run them locally

```bash
# Tier 1 — fastest, no Docker.
make tests

# Tier 2 — needs Docker. Brings up Postgres/Redis/Kafka/ClickHouse,
# runs e2e tests against host-spawned subprocess.
make e2e

# Tier 3 — needs Docker. Builds the actual image, brings up full
# compose stack, drives a charger flow through the running container.
make compose-smoke
```

## How they map to CI jobs

```text
.github/workflows/:
  quality.yml         ─►  lint, types, config-reference-fresh, proto-breaking
  tests.yml           ─►  unit                (Tier 1, every push)
                          mock-backend-smoke  (subprocess smoke for the in-repo mock)
  e2e.yml             ─►  e2e                 (Tier 2, every push)
  compose-smoke.yml   ─►  compose-smoke       (Tier 3, PRs touching deploy/* + main)
  docs.yml            ─►  build               (tagged-release docs site)
```

## Adding a new test — which tier?

- I'm asserting a function returns the right value → **Tier 1**.
- I need a real Postgres / Redis / Kafka / ClickHouse to make my
  assertion meaningful → **Tier 2**.
- I need to assert that the **packaged container** does (or doesn't
  do) something → **Tier 3**.
- I need to assert that the Helm chart / kustomize overlay works →
  **Tier 4** (file an ADR; this tier doesn't exist yet).

If you find yourself reaching across tiers — e.g., your Tier 1 test
spawns a subprocess, or your Tier 3 test reads internal state via a
backdoor — that's a smell. Move the assertion to the tier whose
contract it actually belongs to.

## Anti-patterns

- **Silent skips.** A test that skips because a service is unreachable
  is invisible in CI. Tier 2 and Tier 3 both honour `E2E_REQUIRE=1` —
  in that mode, unreachable services hard-fail the test instead of
  skipping. CI always sets `E2E_REQUIRE=1`.
- **Mocking the failure mode.** Mocking Redis to return `None` proves
  your code handles `None`. It does not prove the real Redis ever
  returns `None` for a malformed value. Mock at the boundary, not at
  the dependency under test.
- **Using `time.sleep` to "let things settle".** A flake masquerading
  as a slow test. Poll for the condition you actually want, with a
  timeout. The smoke tests use `_wait_until` helpers — copy them.
- **Coverage as a stand-in for confidence.** 100 % unit coverage of a
  function that's never invoked at runtime is worth zero. Tier 3
  exists because coverage stops being a useful proxy at the binary
  boundary.
