# ADR-0024: Test trust ladder — what "tests pass" must guarantee

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-05 | Mostafa |

## Context

This service had a green CI pipeline (289 unit tests passing, 86 % coverage,
ruff + mypy clean, e2e job green) at the same time that **the deployable
artifact crashed on startup**. The `eveys-ocpp` container exited within
seconds of `docker compose up` because the gateway, defaulting Kafka to
`localhost:9092` inside its own network namespace, could not bootstrap.
Worse, the `clickhouse-ingestor` sidecar appeared "Up" for weeks but was
silently running the gateway's entrypoint instead of the ingestor module
— a Dockerfile `ENTRYPOINT` shape that swallowed the compose `command:`
override as positional argv.

Neither bug was in Python code. Both were in the deploy-time wiring:
`docker-compose.yml` and `Dockerfile`. The entire test pyramid below
the e2e job is incapable of seeing them, because the unit suite mocks the
data plane and the e2e job uses GitHub Actions `services:` containers
that do not go through the Dockerfile or compose file at all.

This ADR defines the minimum number of test tiers we keep, and what each
tier is **contractually responsible for catching**, so that "CI is green"
once again means "the binary I am about to ship boots and stays up."

## Decision

We adopt a four-tier test trust ladder. Each tier has a single, named
responsibility. A bug class falls in exactly one tier's territory. If a
class of bug has no tier, we add one — we don't bolt the check onto a
tier that was designed for something else.

| Tier | Lives at | CI job | What it proves | What it does NOT prove |
|---|---|---|---|---|
| 1. Unit | `tests/unit/` | `tests.yml::unit` | Pure-Python logic. Handlers compute the right reply, repositories build the right SQL, the Authorize cache is defensive against malformed values, the bus serialises correctly. Imports parse. Coverage gate: ≥ 80 %. | Nothing about real Postgres / Redis / Kafka / ClickHouse. Nothing about the binary booting. Nothing about deploy-time wiring. |
| 2. Integration with services | `tests/e2e/` (GitHub Actions `services:`) | `e2e.yml` | The Python code talks to **real** Postgres / Redis / Kafka / ClickHouse correctly. Schemas migrate. The Kafka → ClickHouse pipeline materialises rows. Two-pod gRPC dispatch round-trips. The local pytest process drives a charger simulator against an in-process gateway. | Whether the **packaged container image** boots. Whether `docker-compose.yml` works. Whether the Dockerfile entrypoint does what its author thought. |
| 3. Container compose-smoke | `tests/compose_smoke/` | `compose-smoke.yml` | The `eveys-ocpp:dev` image built by `deploy/Dockerfile` boots, opens its WS+gRPC sockets, and stays up under the **production-shaped** `docker-compose.yml`. The `clickhouse-ingestor` sidecar stays up under the same compose. A real OCPP charger can complete BootNotification → Authorize → StartTransaction → MeterValues → StopTransaction against the host port and the rows materialise in Postgres + ClickHouse. | Whether Helm charts are correct. Whether prod secrets are wired. (Tier 4, future: kind/k3d tier when Phase 4 lands `deploy/k8s/`.) |
| 4. Production-shape (future) | `tests/k8s_smoke/` (TBD) | `k8s-smoke.yml` | Helm chart from `deploy/k8s/` deploys cleanly into a kind/k3d cluster, Pods reach Ready, charger simulator drives a session against the cluster ingress. | Real cloud secret managers / mTLS / IAM — out of scope for any local tier. |

The **key invariant** this ladder enforces:

> Every byte of YAML or Dockerfile that ships to production must be
> exercised by a test that fails when the byte is wrong.

Tiers 1 and 2 have always existed. Tier 3 is what this ADR adds. Tier 4
is held open for Phase 4.

### Tier 3 in detail

`tests/compose_smoke/` is not a unit test, not a pytest fixture against
sidecars. It does the following from a developer workstation or CI runner
that has Docker installed:

1. Run `docker compose -f deploy/compose/docker-compose.yml up -d --build`
   — the **same compose file and Dockerfile** that ship.
2. Wait for every container to reach `running` state. **Crash-loop and
   silent-restart count as failures**: `restart_count > 0` after the
   stabilisation window fails the test even if the container is
   currently `Up`. This is what catches the case where a misconfigured
   ingestor exits → docker restarts it → exits again, indefinitely.
3. Wait an additional 15 s after each container is `Up` and re-check
   `restart_count`. A container that exits 10 s into life is a startup
   bug, not a transient flake.
4. Drive a real OCPP 1.6 charger simulator (`websockets` client, the
   same `mobilityhouse/ocpp` library production uses) against the host
   port `19000`. Boot, authorise, start a transaction, send meter
   values, stop. Assert: Postgres row in `transactions`, ClickHouse
   row in `cp_meter`, no errors logged by either container.
5. Tear down with `docker compose down --volumes`.

This tier is **slower** (~ 90 s of the CI budget) and **needs Docker**,
which is why we don't run it on every push by default — it's bound to
the same triggers as `tests:e2e` (default branch + MRs touching
`deploy/`, `tests/compose_smoke/`, or `pyproject.toml`).

### What goes wrong without Tier 3

Concretely, the bugs Tier 3 catches that no other tier sees:

| Bug class | Example | Who sees it |
|---|---|---|
| `ENTRYPOINT` / `CMD` shape | Compose `command:` silently appended to combined entrypoint, wrong module runs, container "Up" but doing nothing | Tier 3 |
| Missing env var on a service | `EVEYS_OCPP_KAFKA_BROKERS` not set on `ocpp` service → defaults to `localhost:9092` → unreachable inside container | Tier 3 |
| Kafka listener / advertisement layout | Single `localhost:9092` advertisement → in-network clients bootstrap then fail every metadata-driven request | Tier 3 |
| Health-check command using a binary not present in image | `wget` flag mismatch BusyBox vs GNU; container reports unhealthy forever | Tier 3 |
| Image missing a runtime file | A library used at boot dropped from `[runtime]` deps | Tier 3 |
| Python startup ordering | Producer started before settings finalised | Tier 3 |
| Postgres / Redis / Kafka / ClickHouse client speaking the wrong protocol version | Schema mismatch with the image we pin | Tier 2 (also visible in 3) |
| Pure handler logic bug | Authorize returns wrong status | Tier 1 |

Without Tier 3 the first six rows ship green.

## Consequences

### Positive

- "Tests pass" recovers its plain meaning: a green pipeline implies the
  artifact will boot under the compose configuration we publish.
- New deploy-time changes (new env var, new container, new healthcheck)
  cannot be merged without a Tier 3 run that sees the change. The
  pipeline becomes the spec for compose correctness.
- The bug discovered during the audit (`ENTRYPOINT` shallowing
  `command:`) is now permanently fixable without recurrence: a future
  refactor that breaks the same wiring fails Tier 3.

### Negative

- Tier 3 needs Docker on the runner. GitHub-hosted ubuntu runners ship
  Docker preinstalled — no infrastructure ask.
- Adds ~ 90 s to PR pipelines that touch `deploy/`. Acceptable: the
  alternative is shipping a broken image.
- Tier 3 is heavier to debug than a unit test. A failure shows up as
  "container exited", and the developer needs to read container logs.
  We mitigate by capturing all container logs as CI artifacts (so a
  failure on a remote runner produces the same signal as locally).

### Rejected alternatives

- **Add a healthcheck on the `ocpp` service and let compose's
  `depends_on: condition: service_healthy` catch it.** Healthchecks
  are great for orchestration but not for tests: a container that
  is healthy in dev because it never tried to publish to Kafka can
  still crash on first publish. Tier 3 *uses* the actual code paths.
- **Promote `tests/e2e/` to use the built image.** That would conflate
  the "does the Python talk to real services" concern (Tier 2) with
  the "does the container start" concern (Tier 3). Two tiers, two
  failure modes, two debug paths is clearer than one omnibus tier
  that mixes everything.
- **Run a periodic out-of-band smoke job and ignore on PRs.** Defeats
  the purpose: a regression introduced on Monday that breaks the
  nightly job blocks deploys until Wednesday. Pre-merge is the only
  point at which "tests pass" can mean what we want it to mean.

## References

- Bug audit that motivated this ADR (2026-05-05): the `eveys-ocpp`
  container exited on `localhost:9092` Kafka bootstrap; the
  `clickhouse-ingestor` had been running the gateway entrypoint for
  weeks under a silently-overridden compose `command:`.
- [docs/10-testing-strategy.md](../10-testing-strategy.md) — the
  operational guide for running each tier.
- [ADR-0019](./0019-kafka-producer-hardening.md), [ADR-0020](./0020-clickhouse-ingestion-sidecar.md) —
  the producer + ingestor whose deployment shape Tier 3 now verifies.
