# ADR-0013 — Retention and archival for ClickHouse telemetry

- **Status**: Deferred
- **Date**: 2026-05-08
- **Author**: Eveys engineering
- **Reviewers**: Project tech lead

## Context

The gateway writes four time-series tables to ClickHouse via the
ingestor sidecar (ADR-0020):

- `cp_meter` — every `MeterValues` sample. Highest-volume table by
  far; row count grows linearly with fleet size and session density.
- `cp_status` — every `StatusNotification` state transition.
- `cp_boot` — every `BootNotification`.
- `tx_started` — every `StartTransaction` (the matching transaction
  totals live in Postgres, not here).

All four are `MergeTree`, partitioned monthly on `occurred_at`, and
**have no `TTL` clause today**. Data accumulates indefinitely.

That is fine for now: the fleet is small, disks are nowhere near full,
and we have no production deployment yet. It will not be fine
forever — at fleet scale, an unbounded `cp_meter` table becomes
expensive to store and slower to scan, and operators will eventually
free space under pressure, which is the moment unintentional data
loss happens.

The longer-term shape we want is a **hot/cold tiered store** so we
keep recent data in ClickHouse for fast queries and offload older
data to cheap object storage for audit / long-tail access. The exact
shape is not decided yet, and we are deliberately not deciding it in
this ADR — the production fleet is months away and picking now would
lock in assumptions we cannot validate.

This ADR exists to **(a) record the deferral** so the question is not
forgotten, **(b) name the candidate direction** so the next owner has
a starting point, and **(c) state explicitly that no retention /
delete TTL is applied today**, so nobody silently adds one before the
design lands.

## Decision

- **No `TTL DELETE` clause is added to any ClickHouse table at this
  time.** Telemetry retains indefinitely until ADR-0013 is upgraded
  from Deferred to Accepted with a concrete design.
- The candidate direction (not yet committed) is **two-tier**: ClickHouse
  as the hot tier with a bounded TTL on the order of weeks, and an
  object-storage cold tier (Parquet on S3 / MinIO) for long-term
  retention, with a unified query layer over both.
- A follow-up ADR replaces this one — or extends it in-place by
  flipping the status — when one of the trigger conditions below
  fires.

## Trigger conditions (when to revisit)

Don't write the design speculatively. Pick it up when **any** of
these is true:

1. **Disk pressure**: ClickHouse storage on the production cluster
   exceeds 60% of provisioned capacity, OR ingest rate forecasts hit
   the same threshold within one quarter.
2. **Query latency regression**: P95 latency on the most-used
   time-range queries (per `docs/14-slos.md`) drifts >2× baseline
   and the regression traces to scan size on `cp_meter`.
3. **Audit / compliance ask**: a regulator, customer, or finance
   team asks for "give me everything for charger X over the last
   N years" where N exceeds whatever in-cluster retention we
   eventually pick.
4. **Cost ask**: finance flags ClickHouse storage spend as a top-N
   line item.

Until one of these fires, the work is **deferred**, not "in
progress" or "next". The Phase 5 hardening work (E5-* in
`docs/06-implementation-plan.md`) does not block on this.

## Candidate direction (sketch, not a commitment)

The shape we have in mind, written down so a future owner has
something to react to:

- **Hot tier — ClickHouse**: `TTL occurred_at + INTERVAL 30 DAY DELETE`
  (or whatever number the design lands on) on `cp_meter`; longer
  for the lower-volume tables. Recent data stays fast to query.
- **Cold tier — Parquet on object storage** (S3 in production,
  MinIO in dev/staging): partitioned by `cp_id` and date, written
  out in batches.
- **Write path**: each event is durable in **both** tiers before
  the ingestor advances its Kafka offset. The mechanism is the
  open design question:
  - Option A: dual-write from the ingestor (one path to ClickHouse,
    one to Parquet via pyarrow / object-storage SDK).
  - Option B: single-write to ClickHouse, then periodic export of
    aged partitions to Parquet via `clickhouse-client SELECT ...
    FORMAT Parquet` or `EXPORT` once supported.
  - Option C: a stream processor (e.g. Flink / Benthos) tailing the
    same Kafka topics and writing Parquet directly, so the cold
    tier doesn't depend on ClickHouse being healthy.
- **Read path — Trino (or alternative)**: a federated query layer
  fronts both ClickHouse and the Parquet store. Queries are routed
  by partition age — recent partitions in ClickHouse, older
  partitions in Parquet — and a single SQL query spans both
  transparently. The `cp_meter` schema is mirrored across the two
  tiers so the query shape is identical.
- **Backups remain a separate concern**: `clickhouse-backup` to
  object storage for the hot tier, object-storage native versioning
  for the cold tier. Backups protect against corruption / operator
  error; the cold tier is the long-term archive.

That sketch is **illustrative**. Specifically open: choice of
federated query engine (Trino vs. an alternative like ClickHouse's
own external-table support), dual-write vs. async-tier mechanism,
and whether the cold tier is keyed by `cp_id`/date partitions or
something else. None of those answers should be picked in this
ADR.

## Alternatives considered

- **Pick a TTL number and ship now** (e.g. `TTL occurred_at +
  INTERVAL 18 MONTH DELETE` on every table). Rejected for now: at
  current scale we do not have evidence to pick the number, and
  setting it too aggressively risks deleting data a future audit
  needs. "Infinite until evidence forces a choice" is the safer
  default while pre-production.
- **Pure ClickHouse with very long retention** (e.g. 5 years of
  `cp_meter` in MergeTree). Rejected as the long-term answer:
  storage cost grows fastest in the highest-volume table, and
  ClickHouse is not the cheapest place to keep data that is rarely
  queried. Acceptable as a stopgap if the cold tier slips.
- **ClickHouse storage tiers (`storage_policy` with hot/cold disks
  inside the same cluster)**. Rejected as the long-term answer:
  still keeps data inside ClickHouse, still scales storage cost
  with cluster footprint, and doesn't give us a separate audit
  surface. Worth revisiting as a *bridge* if the full Trino +
  Parquet design takes longer than expected.
- **Move everything to Parquet immediately, drop ClickHouse for
  reads**. Rejected: ClickHouse's per-charger time-range scans
  are the gateway's read pattern (see `docs/14-slos.md`), and
  Parquet over object storage is meaningfully slower for that
  workload at small partition sizes.

## Consequences

### Positive

- We do not lose data to a TTL we picked without evidence.
- The retention question has a documented home rather than living
  in the heads of whoever happens to be on-call.
- Future owners get a starting sketch they can accept, modify, or
  reject — they are not starting from zero.

### Negative / costs

- ClickHouse storage grows unbounded until a trigger condition
  fires. The trigger conditions are explicitly defined to catch
  this before it becomes an emergency.
- The lack of a cold tier means an audit ask that arrives
  earlier than expected will require a rushed implementation.
  Mitigated by: low probability while pre-production, and
  trigger condition #3 names this case so it is at least
  noticed.

### Risks

- **Operator under pressure deletes partitions manually** to
  free space, on a cluster without backups. Mitigation: ADR-0013
  upgraded to Accepted (with a real TTL or backup story) before
  production goes live; the Phase 5 / E5-10 DR drill must not
  pass until backups exist (separate ADR / task, not this one).
- **Trigger conditions go unwatched.** Mitigation: dashboard
  panel for ClickHouse disk usage already exists in the Phase 4
  observability work; an alert at 60% closes the gap. That alert
  is a one-line addition, not a new ADR.

### Reversibility

Fully reversible. This ADR commits to nothing other than "do not
add a `TTL DELETE` yet". Adding TTL later is a single migration
per table and only affects partitions older than the chosen
threshold; data younger than the threshold is unaffected. The
hot/cold tiering design, when it lands, is a larger change but
the migration path is incremental: stand up the cold tier in
parallel, dual-write, prove read parity, then add the TTL on the
hot tier.

## Operational note (today)

Anyone reading this in 2026-Q2 should know:

- ClickHouse currently runs as a **single replica** in dev
  compose; production topology is not yet decided.
- There are **no automated backups** of ClickHouse today. Disk
  loss = total telemetry loss. Postgres (which holds
  billing-critical state — transactions, charge points,
  reservations) has its own backup story owned by the platform
  team and is out of scope for this ADR.
- Kafka retention (default 7 days) is the practical ceiling on
  "how long can the ClickHouse ingestor be down before data is
  permanently gone, even if the box itself never lost a byte."
  Worth verifying before Phase 6 staging soak.

These three items are flagged here for completeness; they are
**not** decided by this ADR. Each should land in its own focused
work item (ClickHouse backups belong with E5-10 DR drill;
production topology lands with E5-1 / Helm chart; Kafka retention
verification is a one-line ops check).

## References

- [ADR-0004](./0004-clickhouse-timeseries-store.md) — why ClickHouse for the time-series tier.
- [ADR-0020](./0020-clickhouse-ingestion-sidecar.md) — the ingestor that writes to these tables.
- [ADR-0028](./0028-ingestor-fail-fast-policy.md) — the fail-fast policy that bounds data loss to "Kafka retention window minus ingestor downtime".
- `src/eveys_ocpp/clickhouse/ddl/0002_create_cp_meter.sql` — current `cp_meter` DDL (no TTL).
- `src/eveys_ocpp/clickhouse/ddl/0003_create_cp_status.sql` — current `cp_status` DDL (no TTL).
- `src/eveys_ocpp/clickhouse/ddl/0004_create_cp_boot.sql` — current `cp_boot` DDL (no TTL).
- `src/eveys_ocpp/clickhouse/ddl/0005_create_tx_started.sql` — current `tx_started` DDL (no TTL).
- `docs/06-implementation-plan.md` E5-10 — DR drill (where ClickHouse backup tooling lands, not this ADR).
