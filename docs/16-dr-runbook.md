# 16 — Disaster-recovery runbook

> **What this is.** A drill kit + execution guide for proving the
> gateway's resilience claims. The roadmap's Phase 5 / E5-10 line —
> "DR drill: kill DB / Redis / pods → service recovers without
> data loss" — splits cleanly into a drill *kit* (this doc) and a
> formal staging exercise (Phase 6 gate review). The kit is what
> validates the runbook works; the exercise is what runs it on
> production-shaped infrastructure.

## What's in the kit

Three drill scenarios, all under `tools/load/scenarios/`:

| Scenario | What it kills | What it asserts | E5-10 mapping |
|---|---|---|---|
| `postgres_kill` | Postgres primary | SLO 4 — every acked transaction is in the table after recovery | "kill DB primary" |
| `redis_kill` | Redis | WS connections survive; idempotency / rate-limiter / authorize-cache fail open per their ADRs | "kill Redis" |
| `reconnect_storm` (E4-7) | 50% of WS connections | Fleet recovers within 60 s, boots reach pre-storm levels | "kill 1/3 pods" — same shock, different lever; see § "Pod-kill on multi-pod" |

All three live in the same `tools/load/` runner so a single command runs the whole drill battery.

## Running the kit

### Quick mode (~2 minutes, runs against `make compose-up`)

```bash
make compose-up   # bring the local stack up
.venv/bin/python -m tools.load --quick --scenario postgres_kill --scenario redis_kill
```

A pass/fail Markdown report goes to stdout. Add `--out report.md` to write to a file, `--json` for machine-readable output.

The drill scenarios stop and start docker compose services (`docker compose stop postgres`, then `start postgres`). They restore the service in their `finally` block even on crash, so a partial run never leaves the local stack broken.

### Full mode (longer windows, designed for staging)

```bash
.venv/bin/python -m tools.load --full --scenario postgres_kill --scenario redis_kill \
    --target wss://ocpp.staging.example.com
```

Same commands, larger fleet sizes and longer outage / recovery windows. The `--target` argument points the simulator at any reachable WS URL; the `docker compose ...` operations target whatever the **local** compose file resolves to. Running `--full` against a remote target is only useful when the target's stack lives on the same host as the runner (e.g. an SRE laptop with a port-forwarded tunnel into staging) — for true multi-pod staging drills, run the scenarios from inside the cluster (a debug pod) so `docker compose` resolves to the cluster's local services.

## What "pass" means per scenario

### `postgres_kill`

- **Postgres healthy after restart** — `docker compose ps postgres` reports healthy within 30 s of the restart command.
- **Acked transactions survived the kill** — `COUNT(*) FROM transactions WHERE cp_id LIKE prefix%` is at least the simulator's `counters.transactions` count at kill time.

A failure on the second criterion is a billing incident waiting to happen and gets escalated immediately. See [`docs/14-slos.md`](./14-slos.md) SLO 4 (transaction durability = 100%, zero-tolerance).

### `redis_kill`

- **WS connections survive Redis outage** — fleet `counters.connected` mid-outage is ≥ 95 % of the settled level.
- **WS connections steady after recovery** — same threshold post-recovery.
- **Error counter stays below fail-open budget** — `counters.errors` during the outage stays below `count / 5` (or 5, whichever larger).

A failure here means a fail-open path regressed into a fail-loud one. Likely culprits, in order of probability:

1. The idempotency cache (ADR-0017) started raising on Redis errors instead of swallowing them.
2. The rate limiter (E5-3) started denying instead of fail-open.
3. The Authorize cache (E3-4) started raising on Redis errors.
4. The registry's `mark_online` is in the WS connect path — if it raises, the connection dies. (It should swallow, but: regression-prone.)

### `reconnect_storm` (E4-7)

Pre-existing scenario, used as the third drill from E5-10's perspective. See [`docs/13-load-testing.md`](./13-load-testing.md) § Reconnect-storm for the criteria.

## When a drill fails

Order of operations:

1. **Don't merge the change that caused it.** The drill kit is a CI-grade gate against regressions in resilience invariants; if a PR turned a fail-open path into a fail-loud one, that's a deliberate rollback.
2. **Capture the simulator output** (the Markdown report or `--json` stream) as the postmortem evidence.
3. **Inspect logs for the gateway service**: `docker compose logs ocpp` will show whether the failure traces to a specific code path (e.g. an unhandled exception in a handler that should have caught and continued).
4. **For `postgres_kill` durability failures specifically:** treat as a P0 — billing data integrity. Page on-call (when on-call exists per Phase 7); until then, escalate to the tech lead.

## Pod-kill on multi-pod

The roadmap line "kill 1/3 of pods" can't be exercised on the local single-pod compose stack — there's only one pod. Two paths cover this:

- **Local approximation**: the existing `reconnect_storm` scenario (E4-7) drops 50 % of WS connections, which is the *user-visible* shock a pod kill produces. Per-charger reconnect timing and cache replay behaviour are exercised. Cross-pod registry rebalance and gRPC routing are NOT.
- **Real exercise**: run the same scenario against a multi-pod deployment (k3d locally, or staging once it exists). The simulator's `--target` switch is all that needs to change.

When the Helm chart lands (E5-1), the runbook here gets a section on `kubectl delete pod` against the gateway deployment.

## What this kit is **not**

- **Not the formal Phase 6 staging-soak exercise.** That's a scheduled event run on production-shaped infrastructure with the full team in the room, results recorded in a postmortem doc. The drill kit is what validates the runbook's commands and pass criteria work; the exercise is what runs them in anger.
- **Not a substitute for backups.** Postgres point-in-time recovery, ClickHouse `clickhouse-backup` to S3 (per ADR-0013), Kafka retention — those are separate concerns. The drill validates that the gateway *behaves* correctly under failure; it does not validate that the data is recoverable from a destroyed disk.
- **Not a security-regression test.** Pen test (E5-9) and CVE scanning (E5-8 / `pip-audit`) cover that ground separately.

## Adding a new drill

A new drill is a new file in `tools/load/scenarios/` plus one line in `_SCENARIOS` in `tools/load/__main__.py`. Match the existing pattern:

- A frozen-dataclass `Config` with the scenario's parameters.
- An `async def run(config) -> ScenarioResult` function that does the kill / restart / observe sequence.
- `async def run_quick(target_url, prometheus_url) -> ScenarioResult` and `run_full` for the runner's `--quick` / `--full` modes.
- Pass criteria expressed as `Criterion` records — each one is a row in the report's pass/fail table.

Use `tools/load/_docker_helpers.py` (`stop`, `start`, `wait_healthy`) for compose-service control. For non-compose targets (k3d, real staging), add a sibling `_kube_helpers.py` and route based on the scenario's target.
