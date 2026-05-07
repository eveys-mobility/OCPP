# Load testing

Drive the gateway at production-shaped scale and capture pass/fail evidence per the [roadmap](./01-roadmap.md) Phase 4 exit criteria.

> **Status (2026-05-07):** v0 rig — one scenario (`boot_storm`) and a `--quick` mode that runs against `make compose-up` in under 2 minutes. The headline 10k-charger / 1-hour run is wired (`--full`) but is documented as an operator-on-staging task, not a CI gate.

## What's in the rig

The rig lives at `tools/load/`. Like `tools/sim/` (see [E4-5](./02-tasks.md)) it is **not** shipped in the production wheel — invoke from a dev checkout.

| File | Purpose |
| --- | --- |
| `tools/load/__main__.py` | CLI entry — `python -m tools.load --quick`, `--scenario X`, `--json`, `--out FILE` |
| `tools/load/run.sh` | Thin bash wrapper so the spec's `tools/load/run.sh --quick` resolves to a real script |
| `tools/load/scenario.py` | `ScenarioResult` + `Criterion` dataclasses; the contract every scenario emits |
| `tools/load/scenarios/boot_storm.py` | v0 scenario — N chargers connect inside a window, then hold |
| `tools/load/prometheus.py` | 60-line `httpx`-based instant + range query client |
| `tools/load/report.py` | `ScenarioResult[]` → Markdown report renderer |

## Quickstart

Bring up the local stack and run the rig:

```bash
make compose-up
# alembic upgrade head — only needed once per fresh DB
alembic upgrade head

# Quick smoke (≤2 min, 10 chargers / 10 s each scenario)
tools/load/run.sh --quick

# Override target / Prometheus URLs
LOAD_TARGET=ws://gateway.staging:9000 \
LOAD_PROMETHEUS=https://prom.staging.internal \
  tools/load/run.sh --quick

# JSON output for CI parsing
python -m tools.load --quick --json --out report.json
```

A typical `--quick` report looks like:

```markdown
# Load test report

**1 of 1 scenarios passed.**

## boot_storm — PASS
Started `2026-05-07T11:00:00+00:00`, ran for `12.4s`.

| Criterion | Threshold | Actual | Result |
| --- | --- | --- | --- |
| all chargers booted | `>= 10` | `10` | PASS |
| BootNotification P99 latency | `< 3.0s` | `0.084s` | PASS |
| no fleet-side errors | `== 0` | `0` | PASS |
```

## Pass criteria (from the roadmap)

The rig measures these explicitly:

| # | Criterion | Status in v0 |
| - | --- | --- |
| 1 | 10k concurrent chargers maintained for 1 hour | covered by `boot_storm --full`; not a CI gate |
| 2 | 1k transactions/minute sustained | scenario TODO (`steady_traffic`) |
| 3 | P95 RemoteStart end-to-end < 3s | scenario TODO (`remote_start_latency`) |
| 4 | Zero lost transactions | scenario TODO (`durability`) |
| 5 | Postgres pool not exhausted | covered indirectly by `boot_storm` (no fleet errors); explicit scenario TODO |
| 6 | Kafka consumer lag < 30s steady-state | scenario TODO (`webhook_lag`) |
| 7 | Fleet recovers within 60s after 50% pod kill | covered by `reconnect_storm --full` (E4-7) |

The "TODO" scenarios are deliberate gaps — adding each is one new file under `tools/load/scenarios/` plus one line in `__main__._SCENARIOS`. The rig is built so the next operator/engineer can land a scenario in an hour.

## Scenarios

### `boot_storm`

N chargers connect inside a window, then idle. Asserts:

- All chargers booted (proxy for "fleet ramped to size")
- BootNotification handler P99 latency stays under threshold
- No fleet-side errors during the run

`--full` shape: 10k chargers, 5-minute ramp, 1-hour hold.

### `reconnect_storm` (E4-7)

Settle → drop 50% of WS connections → measure recovery. Asserts:

- Fleet settled to ≥95% of count before the storm
- Re-boots arrive within the recovery window (default 60s)
- Each dropped charger booted again

**Single-pod compose limitation.** A single-pod compose stack exercises the **WS-layer** part of the recovery story (re-register storm, idempotency-cache absorption, per-pod registry gauge) but **not** the cross-pod registry rebalance that fires when an actual pod dies. For the cross-pod path, run the same scenario against a multi-pod k3d / staging deployment — the scenario code is identical, only `--target` changes.

`--full` shape: 2k chargers, 60s settle, 60s recovery window.

## Where the simulator runs

10k chargers from one machine is right at the edge for a laptop (websockets per-connection memory + asyncio task overhead). The v0 rig assumes **single-machine** operation.

- Laptop: aim for **`--quick` only** (10 chargers).
- Staging box: **`--full`** with up to ~10k chargers depending on RAM/CPU. A 32 GB / 8-core box is a sensible minimum; the workload is I/O-bound, not CPU-bound.

Multi-machine simulator orchestration is a Phase 4 follow-up; if `--full` on one box can't reach 10k, file an issue referencing this section and we'll spec it out.

## Where the gateway runs

For meaningful results the gateway must run **production-shaped**, i.e. the same `docker-compose.yml` the [compose-smoke](./10-testing-strategy.md) tier exercises. Three gateway pods minimum so the cross-pod bus path actually gets exercised — the v0 `boot_storm` scenario doesn't need this, but the to-do `remote_start_latency` scenario will.

`--quick` against a single in-process gateway (the `running_service` fixture from `tests/e2e/test_local_smoke.py`) is fine for sanity-checking the rig itself, but not for capturing evidence — single-pod numbers don't generalise.

## Out of scope (deferred to follow-up tasks)

- **Multi-machine simulator orchestration.** v0 expects one box.
- **k3d / kind cluster targeting.** Compose-only for now. The rig accepts arbitrary `--target` / `--prometheus` URLs so a k3d run is a config change, not a code change — but no orchestration script ships in v0.
- **Grafana screenshot rendering.** The Markdown report links to the [Grafana fleet dashboard](../deploy/grafana/dashboards/01-fleet-overview.json) by `uid` (`eveys-ocpp-fleet`); inlining PNGs needs a Grafana service account credential the rig doesn't ask for.
- **CI gating on a green `--full` run.** Way too slow for CI. The rig captures a baseline; regressions are reviewed manually against the report.

## Adding a scenario

1. Create `tools/load/scenarios/<name>.py` exposing async `run_quick(target, prometheus)` and `run_full(target, prometheus)` returning a `ScenarioResult`.
2. Add `<name>: <module>` to `_SCENARIOS` in `tools/load/__main__.py`.
3. Add a unit test under `tests/unit/load/` that exercises the scenario's pass/fail logic against canned Prometheus responses (don't depend on a real Prometheus in the unit suite — the rig already has `tests/unit/load/test_prometheus_client.py` as a template).

Each scenario should:
- Use the simulator (`tools/sim`) for traffic so the load shape is reproducible across boxes.
- Pull *derivative* metrics from Prometheus (handler P99, consumer lag, pool gauges) for assertions — don't re-implement what `eveys_ocpp_*` already exposes.
- Emit `Criterion` rows with the actual measurement expression in the `expression` field so reviewers can re-run the query.
