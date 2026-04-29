# ADR-0004 — ClickHouse as the time-series store

- **Status**: Accepted
- **Date**: 2026-04-29
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

OCPP chargers emit a high volume of small, append-only records:

- **`MeterValues`** — energy/power/current/voltage samples, typically every 5–60 seconds per active connector. At 10k chargers, this is ~10⁵–10⁶ rows/minute steady-state.
- **`Heartbeat`** — once every 30–300 seconds per charger. At 10k chargers, ~30–300 rows/second.
- **`StatusNotification`** — bursty, especially during reconnect storms.

This data is **append-only**, **time-ordered**, and queried by time range / charger / connector — never by primary key. Storing it in Postgres causes table bloat, query slowdown, and contention with the OCPP request path (the existing pain pattern). We need a store that:

- Ingests millions of rows per minute without backpressure on the Kafka producer.
- Compresses time-series data efficiently (typical ratio ≥ 10× vs row-store).
- Answers per-charger / per-connector / per-time-range queries in seconds.
- Has a Python async client that integrates cleanly with our `asyncio` stack.

Constraints:

- Must be open-source-licensed and self-hostable (we may move to a managed service later, but day one is in-cluster).
- Must run on Kubernetes alongside the rest of the platform.
- Must support SQL for ad-hoc analytics — engineers should not need a new query language.

## Decision

**ClickHouse is the time-series store for `MeterValues`, `Heartbeats`, `StatusNotifications`, and any future high-cardinality telemetry.**

Data flow: charger → OCPP handler → Kafka → ClickHouse (downstream consumer). `eveys/ocpp` itself never writes to ClickHouse directly — it produces to Kafka, and a separate consumer (initially a sidecar, later possibly a `Kafka Engine` table on the ClickHouse side) lands the data.

Postgres remains for **transactional** state (`charge_points`, `transactions`, configuration). ClickHouse is **never** queried on the OCPP request path — only by analytics, billing, and operations dashboards.

## Alternatives considered

- **TimescaleDB** — Postgres extension, easier operationally if the team already runs Postgres. Rejected: at the row volumes we expect, ClickHouse outperforms TimescaleDB by ~10× on ingest and ~3–5× on typical scan queries. We don't get an operational simplification because the OCPP transactional Postgres has different sizing/backup needs from a TSDB; running them as one cluster couples failure modes.
- **InfluxDB** — purpose-built time-series, good ergonomics. Rejected: InfluxQL/Flux is a separate query language, the OSS license history (relicensing concerns under InfluxDB 3.x) is a risk, and ad-hoc SQL analytics is harder.
- **VictoriaMetrics** — strong Prometheus-compatible TSDB. Rejected: optimized for metric scraping (Prometheus model), not for the high-cardinality per-charger row data we have. Better fit for the operational metrics layer (which is a separate concern, see ADR-TBD on observability).
- **Keep it in Postgres with partitioning** — simplest stack. Rejected: partitioning relieves index size but doesn't fix table bloat, vacuum pressure, or the scan cost on `MeterValues`. The architectural pain we are explicitly avoiding.
- **S3 / object storage with Parquet (data-lake)** — cheapest, archive-friendly. Rejected for the hot-path use case: query latency is minutes, not seconds. Worth revisiting later as a *cold tier* after data ages out of ClickHouse.

## Consequences

### Positive

- Ingest headroom: a small ClickHouse cluster handles the projected fleet (320k chargers) without re-architecting.
- Compression: per-charger telemetry compresses ~10–20× with default codecs; cold storage costs stay manageable.
- SQL: engineers and analysts query ClickHouse directly; no new language.
- Kafka-native: the `Kafka` table engine in ClickHouse can consume topics directly, removing the need for a custom consumer service in many cases.
- Open-source (Apache 2.0): clean licensing, no vendor lock-in.

### Negative / costs

- Operational footprint: ClickHouse adds another stateful service to the platform. Backups, upgrades, replication, and capacity planning are additional ops work.
- Eventual consistency between Kafka and ClickHouse: a row in Kafka does not appear in ClickHouse immediately. Downstream consumers must tolerate seconds of lag.
- Schema migrations: ClickHouse schema changes are not as trivial as Postgres. Adding/removing columns on large tables requires care (`ALTER TABLE ... ON CLUSTER` patterns).
- Two query languages in the platform (Postgres dialect + ClickHouse dialect, mostly compatible but not identical) — minor learning curve.

### Risks

- **Cluster sizing wrong on day one** — ingest backpressure cascades into Kafka lag. Mitigation: load-test during Phase 4 (W6), set replica count and shard plan based on measured throughput, not theory.
- **Schema drift** between producer (`eveys/ocpp` Kafka schemas) and consumer (ClickHouse table). Mitigation: schema registry for Kafka events (already on the ADR backlog: ADR-0014).
- **Query patterns we don't anticipate** force a re-shard. Mitigation: design tables around the *known* query shapes (per-`cp_id`, per-time-range, per-`connector_id`); resist the temptation to make ClickHouse a general-purpose OLAP for the whole platform.
- **Operational expertise** — the team is not yet ClickHouse-fluent. Mitigation: budget two engineers for a one-week training/spike before W4 (when ingest wiring starts).

### Reversibility

- **Reversible at moderate cost.** ClickHouse data is derivable from Kafka (which is the source of truth for events). If we ever needed to switch (to a different TSDB or a managed service), we'd:
  1. Stand up the new store.
  2. Replay the Kafka topics into it.
  3. Cut downstream readers over.
  No data is lost.

## Project conventions implied by this decision

- `MeterValues`, `Heartbeats`, and `StatusNotifications` go to **Kafka**, never to Postgres. (Already encoded in `AGENTS.md` at the repo root, rule 4.)
- `eveys/ocpp` does **not** read from or write to ClickHouse directly. The Kafka boundary is the contract.
- ClickHouse table schemas are versioned alongside the Kafka event protobufs in `proto/events/`.

## References

- [ClickHouse documentation](https://clickhouse.com/docs/en/intro)
- [ClickHouse Kafka table engine](https://clickhouse.com/docs/en/engines/table-engines/integrations/kafka)
- [ADR-0001 — Python 3.13 + asyncio as the primary runtime](./0001-python-asyncio-stack.md)
- ADR-0014 (planned) — Schema Registry choice for Kafka events
