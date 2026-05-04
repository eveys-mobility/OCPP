# ADR-0020 — ClickHouse ingestion: sidecar over Kafka Engine

- **Status**: Accepted
- **Date**: 2026-05-01
- **Author**: Eveys engineering (E2-13 + E2-14; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

ADR-0004 chose ClickHouse as the time-series store for `MeterValues`, `StatusNotifications`, and other high-volume telemetry. ADR-0015 froze the Kafka event envelope. E2-8 wired the gateway to publish those envelopes onto four topics. The remaining work — and the explicit Phase-2 exit gate (`docs/06-implementation-plan.md` line 148) — is "telemetry events visible in ClickHouse within seconds of being published to Kafka".

ADR-0004 left the ingestion mechanism open: *"Kafka Engine table or sidecar consumer if engine constraints push us off it."* This ADR makes that decision concrete and locks in three more conventions that the implementation needs:

1. The actual ingestion path (Kafka Engine vs sidecar).
2. The table engine + partition/order convention for every event-table.
3. The migration tooling (Alembic is Postgres-only and doesn't fit ClickHouse).

Forces:

- The pipeline must be debuggable. We're a small team; an ingest path that surfaces failures only as silent rows-not-arriving has high cost.
- The pipeline must reuse what we already run. We have a healthy `aiokafka` story (E2-7 + E2-8); doubling the substrate count by adding a ClickHouse-native consumer path is a real cost.
- The pipeline must handle schema evolution. Kafka envelopes already declare `schema_version`; the ingestor needs a clean place to branch on it.
- The first version is single-node. We don't have a load-test datapoint yet (Phase 4) and we don't want to over-shape the schemas around imagined cluster behaviour.
- Migration tooling has to play in the same repo as the existing Alembic setup without adding a second migration framework that engineers have to learn.

## Decision

### Ingestion: a Python sidecar consumer

`src/eveys_ocpp/clickhouse/ingestor.py` is a long-lived process that uses `aiokafka.AIOKafkaConsumer` subscribed to the four event topics, parses each `EventEnvelope` with `events_pb2`, branches on the `oneof payload`, batches rows (500 / 5 s, whichever first), and inserts via `asynch` (the de-facto async ClickHouse driver). Deployed as its own service in compose (`clickhouse-ingestor`) and, eventually, in K8s (separate deployment from the gateway pod — independent scale and lifecycle, exactly as ADR-0004 envisioned).

### Table shape: one table per topic, common partitioning convention

- **Engine**: `MergeTree` (single-node for now; ADR-amendment later for `ReplicatedMergeTree` once Phase 4 picks the cluster shape).
- **Partition**: `PARTITION BY toYYYYMM(occurred_at)` — month-level partitions, standard for time-series.
- **Order**: `ORDER BY (cp_id, occurred_at)` — every analytics query we anticipate is per-charger + time-range.
- **Envelope columns** (`event_id`, `occurred_at`, `cp_id`, `schema_version`, `trace_id`) appear on every event-table verbatim. ADR-0015 set them up to be cross-cutting; the storage layer mirrors that.
- **Payload columns** mirror the proto fields by name and type. Proto enums are stored as their string names (e.g. `MEASURAND_VOLTAGE` → `"VOLTAGE"`). String storage costs marginally more than `Enum8`/`Enum16` columns, but: (a) future proto enum additions don't break `INSERT`s with the new value, (b) ad-hoc queries against the table are readable without an enum→ord lookup, (c) the cardinality is low enough that ClickHouse's column compression makes the storage difference negligible.
- **`SampledValue` (repeated in `CpMeter`) becomes a `Nested` type.** Single row per envelope; multiple values queried via `ARRAY JOIN`. The DDL comment names the query pattern so analysts onboarding to the table aren't confused.

### Migration tooling: plain SQL + a tracking table

`src/eveys_ocpp/clickhouse/ddl/NNNN_*.sql` is the source of truth. A 60-line `migrate.py` reads `schema_migrations` (a `MergeTree(version)` table), figures out which DDL files haven't been applied, and runs them in order. The `schema_migrations` shape mirrors Alembic's `alembic_version` deliberately so reviewers familiar with Alembic recognize the pattern; we just don't pull in Alembic itself for ClickHouse.

## Alternatives considered

- **ClickHouse Kafka Engine** — built in, no sidecar process. Rejected: hard to debug (errors land in server logs, not in our structured logging), schema drift requires drop-and-recreate of materialized-view chains, no in-process place to branch on `schema_version`. The sidecar's extra code is bounded; the Kafka Engine's hidden complexity isn't.

- **Kafka Connect sink (Confluent or Aiven `clickhouse-sink-connector`)** — battle-tested, JMX metrics, schema-registry integration. Rejected for now: introduces a third runtime substrate (Kafka Connect cluster) with its own deploy/ops story. Worth revisiting in Phase 4 if the sidecar struggles at fleet scale or if we adopt a schema registry (planned ADR-0014).

- **Per-row `INSERT`s** instead of batches. Rejected: ClickHouse's `MergeTree` is optimized for batched inserts; per-row creates one tiny part per row and crushes the merge worker. The 500-rows-or-5s batch is a standard recipe; this ADR sets the defaults but the values are env-tunable.

- **`ReplicatedMergeTree` / `Distributed` from day one.** Rejected: we have no production load-test yet and the cluster shape (shard key, replica count, ZooKeeper / Keeper) hasn't been chosen. Locking it in now is premature. ADR-amendment when Phase 4 lands.

- **Store proto enums as `Enum16` columns** instead of `String`. Rejected: gain is marginal storage, loss is real schema-evolution friction (every new measurand variant requires a `MODIFY COLUMN`).

- **`Alembic` for ClickHouse**. Rejected: Alembic's whole model is SQLAlchemy + autogenerate diffing, and its ClickHouse support is third-party and partial. Plain SQL files + a tracking table is 60 lines, no new framework to learn.

- **`dbt-clickhouse` or `clickhouse-migrations`**. Rejected for the same reason — both work but the project's bar for new top-level deps is high (AGENTS rule 4) and 60 lines of vanilla Python clears the bar trivially.

- **Materialized views or aggregating tables**. Rejected as not-this-MR: the raw event tables are the contract. Aggregations are a downstream consumer's choice and we don't yet know which aggregations will earn their keep.

## Consequences

### Positive

- Phase 2 exit gate met: events visible in ClickHouse within ~5 s of Kafka publish (one batch cycle).
- Sidecar is unit-testable like the producer: `start()`/`stop()` lifecycle, mockable consumer, one place to branch on `schema_version`.
- Plain SQL DDL keeps reviewers in the language ClickHouse actually speaks. `git blame` on `0001_create_cp_meter.sql` is the schema history.
- Adding a fifth topic later (e.g. a future `cp.heartbeat`) is one DDL file + one `oneof` branch in the ingestor + one row-shape function. No framework gymnastics.
- The convention (`PARTITION BY toYYYYMM(occurred_at), ORDER BY (cp_id, occurred_at)`, envelope columns first) is set once across all four tables and applies to every future event-table by default.

### Negative / costs

- One more deployable: the sidecar is a separate service in compose and will be a separate K8s deployment. Worth the cost (independent scaling, independent failure mode), but it's a process to monitor.
- One new runtime dep: `asynch>=0.2`. Justified per AGENTS rule 4 (sync ClickHouse drivers would block the asyncio loop); flagged in the MR description.
- At-least-once semantics. If the ClickHouse insert succeeds and the offset commit fails, the next poll re-inserts the same batch. Downstream consumers that need exactly-once `GROUP BY event_id` (the envelope already carries it). Documented in the table comment.
- Plain-SQL DDL means no `autogenerate` magic — every schema change is a hand-written file. We see this as a feature; the schema is small and the explicitness is the point.
- Single-node `MergeTree` does not survive a ClickHouse host failure. Acceptable until Phase 4 picks the cluster shape; the data is regenerable from Kafka if we ever need to rebuild.

### Risks

- **`asynch` maintainership.** ~3000 stars, Apache-2.0, one maintainer. Mitigation: pin a known-good version; keep the use surface small (we only use `connect()` + `cursor.execute()` + `cursor.executemany()`); if the project stagnates we can fork or switch to the sync `clickhouse-driver` behind a `loop.run_in_executor` shim with ~50 lines of code.
- **`Nested`-type query ergonomics.** Analysts unfamiliar with ClickHouse may be confused by the `ARRAY JOIN` requirement on `cp_meter.sampled_values`. Mitigation: the DDL comment explicitly cites the query pattern, and the e2e test exercises it.
- **Single-node ClickHouse SPOF**. Acknowledged; Phase 4 sharding/replication is the correction. In the meantime the data is regenerable from Kafka (which itself is replicated).
- **Schema drift.** `schema_version` lives in the envelope but nothing today ties it to ClickHouse's column set. Mitigation: when an envelope variant changes shape (per ADR-0015 § "Project conventions implied by this decision"), the matching ClickHouse DDL changes in the same MR. The `proto-breaking` CI gate (E2-12, ADR-0018) catches the proto side; reviewers catch the DDL side.
- **Batch size vs latency trade-off.** 500 rows or 5 s is a guess. Tunable via Settings. Phase 4 load-test will inform whether to lower the row threshold or shorten the time threshold.

### Reversibility

Reversible at moderate cost. Switching to Kafka Engine, Kafka Connect, or a different async driver is a swap of one Python class behind a stable boundary (the consumer reads protos, writes rows; the production interface is the row shape, not the implementation). Schema choice is reversible at the cost of a `CREATE TABLE AS` + replay-from-Kafka — exactly the model ADR-0004 explicitly designed for ("ClickHouse data is derivable from Kafka").

## Project conventions implied by this decision

- Every new event topic adds: (1) a `oneof payload` variant in `proto/events/v1/events.proto`, (2) a topic name in `Settings`, (3) a producer site (the relevant handler), (4) a DDL file, (5) an ingestor branch, (6) an e2e check.
- Every event-table follows `PARTITION BY toYYYYMM(occurred_at), ORDER BY (cp_id, occurred_at)`. Diverging from this requires an ADR amendment.
- Proto enums become `String` columns in ClickHouse. Diverging requires an ADR amendment.
- Repeated proto fields become `Nested` columns. Same.
- DDL files are append-only; never edit a merged file. To change a table, write a new migration.
- `eveys/ocpp` does not query ClickHouse (per ADR-0004 line 35). The ingestor writes; downstream consumers read.

## References

- [ADR-0004](./0004-clickhouse-timeseries-store.md) — picks ClickHouse as the time-series store.
- [ADR-0015](./0015-kafka-event-envelope-format.md) — the event-envelope contract this ADR's tables mirror.
- [ADR-0018](./0018-grpc-backward-compat-enforcement.md) — the proto-side gate that pairs with the DDL discipline this ADR sets.
- [`proto/events/v1/events.proto`](../../proto/events/v1/events.proto) — the source schema.
- `src/eveys_ocpp/clickhouse/` — implementation directory (DDL files under `ddl/`, plus `migrate.py` and `ingestor.py`); merged in MR !18.
- [`docs/02-tasks.md`](../02-tasks.md) — E2-13 (table schemas) + E2-14 (ingestion path).
- [ClickHouse `Nested` columns](https://clickhouse.com/docs/sql-reference/data-types/nested-data-structures/nested) — the type used for `cp_meter.sampled_values`.
- [`asynch` on GitHub](https://github.com/long2ice/asynch) — the async ClickHouse driver.
