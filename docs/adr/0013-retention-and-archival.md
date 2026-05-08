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

## Recommended retention (deferred — do not implement yet)

The numbers below are the **best-practice recommendation** to
react to when this ADR is upgraded from Deferred to Accepted.
They are **not** the committed schedule, and **no `TTL DELETE`
should be added to any table on the basis of this section
alone**. The next owner adjusts these against the actual fleet
size, query patterns, regulatory advice, and storage budget at
the time the work is picked up.

### Per-table recommendation

| Table | Volume class | Hot tier (ClickHouse) | Cold tier (object storage) | Reasoning |
|---|---|---|---|---|
| `cp_meter` | Very high (every sample of every session) | **30 days** | **7 years** | Recent meter values drive billing dispute resolution and short-term operator queries. Long tail belongs in cheap object storage; the 7-year bound is the rough EU norm for billing-adjacent records. Pick the actual number against the legal team's advice for the deployment region. |
| `cp_status` | Medium (per state transition) | **90 days** | **3 years** | Operator triage usually looks back days-to-weeks ("why was this charger Faulted last Tuesday?"). 90d hot covers that. 3y cold is cheap and helps fleet-reliability post-mortems. |
| `cp_boot` | Low (per power-cycle / reconnect) | **1 year** | **7 years** | Volume is low enough that "1y hot" costs almost nothing. Useful for firmware-rollout retrospectives and charger-replacement audits. |
| `tx_started` | Low (per session) | **1 year** | **7 years** | Same volume class as `cp_boot`. Pairs with the matching transaction record in Postgres, which the platform team retains under its own policy. |

The per-table headline: **`cp_meter` is the only table where hot
retention is short and the cold tier is load-bearing.** Every other
table is small enough that a generous hot retention costs little.
Optimize the design for `cp_meter` and the rest follows.

### Why these numbers (in one paragraph each)

- **30 days hot for `cp_meter`** balances two pressures. Long
  enough that a customer billing dispute filed within the typical
  invoicing cycle (monthly) is served entirely from the hot tier
  with no cold-tier round-trip. Short enough that the table size
  scales with active fleet, not historical fleet — at fleet
  scale, 30d of meter samples is hundreds of GB; 18 months would
  be tens of TB.
- **7 years cold for `cp_meter`** is the safe upper bound for
  billing-adjacent records under common EU rules (GDPR allows
  retention with a documented purpose; tax/billing rules typically
  require 6–10 years). The actual number is a legal call —
  document it against the deployment's jurisdiction. Cold-tier
  storage cost at this duration is negligible compared to the
  ClickHouse cluster cost saved.
- **90 days hot / 3 years cold for `cp_status`** matches operator
  workflow. Reliability post-mortems span months, not years; a
  3-year cold tier covers cross-quarter trend analysis without
  inflating hot-tier scan costs.
- **1 year hot for `cp_boot` / `tx_started`** is generous because
  it costs almost nothing — these tables are orders of magnitude
  smaller than `cp_meter`. Cold tier matches `cp_meter`'s 7-year
  bound for consistency: if a regulator asks for "everything for
  charger X for the past five years", every table answers from
  the same time horizon.

### What gets deleted vs. archived

- `TTL ... DELETE` on the hot tier removes the partition from
  ClickHouse only. The same data is already durable in the cold
  tier (Parquet on object storage) — the delete is **purely a
  hot-tier hygiene operation**, never the only copy of the data.
- The cold-tier expiry (7 years above) is enforced by **object-
  storage lifecycle rules**, not ClickHouse TTL. Object lock /
  versioning protects against accidental deletion within the
  retention window.
- "Never lose data" survives because the hot-tier delete only
  fires after the cold-tier write is confirmed durable. The
  dual-write mechanism (sketched in the section above) is the
  load-bearing piece — getting it right is the prerequisite for
  any TTL DELETE landing in production.

### What is NOT recommended

- **A single TTL number across all tables.** `cp_meter`'s volume
  is two orders of magnitude above the others; treating them
  uniformly either over-retains the small tables (wasted thought)
  or under-retains the small tables (loses cheap-to-keep audit
  data for no gain).
- **Aggressive hot retention (e.g. 7 days) on `cp_meter`.** Saves
  marginal hot-tier storage at the cost of pushing the average
  customer dispute into a cold-tier round-trip. The federated
  query layer hides the round-trip but it is not free —
  Parquet-over-S3 scans are slower than MergeTree scans for the
  per-charger time-range pattern.
- **Indefinite cold retention.** Even cheap storage accumulates,
  and "we kept it because we could" is not a defensible position
  under data-minimization rules. Pick a number, document it,
  enforce it via object-storage lifecycle rules.

### When to revisit these numbers

The numbers above are right for **a billing-adjacent OCPP
deployment in a typical EU regulatory environment, at a fleet
scale that makes `cp_meter` the dominant storage line item.**
Adjust if any of those conditions break:

- Different jurisdiction with shorter / longer mandated record
  retention.
- Non-billing deployment (e.g. fleet-internal monitoring only)
  where the long cold tier is unnecessary overhead.
- Fleet density much higher than expected, where 30d hot on
  `cp_meter` itself becomes the cluster's largest cost.
- Audit / legal ask that names a specific retention obligation
  the recommendation here doesn't already satisfy.

## Recommended stack and approach (deferred — do not implement yet)

Same caveat as the retention section: this is the **best-practice
recommendation** for the next owner to react to, not a committed
implementation. The mechanism below is what we would build today
if asked; the numbers and component choices should be re-validated
against the deployment's actual fleet size, cost envelope, and ops
maturity at the time the work is picked up.

### Component stack

| Layer | Recommended | Why this, not alternatives |
|---|---|---|
| **Hot tier — query** | ClickHouse `MergeTree`, partitioned monthly on `occurred_at` (already in place). | The status quo. Per-charger time-range scans are the dominant read pattern (see `docs/14-slos.md`); ClickHouse is the right shape. |
| **Hot tier — TTL** | `TTL occurred_at + INTERVAL <N> DAY DELETE WHERE 1` per the per-table table above. | Native ClickHouse feature, cheap, runs during background merges. No extra moving parts. |
| **Cold tier — format** | **Apache Parquet**, snappy or zstd compressed, partitioned by `toYYYYMM(occurred_at)` directory and `cp_id` sub-partition. | Self-describing, columnar, native to every analytics engine. Outlives whatever query engine we run today. Avoid Avro / proprietary formats. |
| **Cold tier — storage** | **S3 in production, MinIO in dev/staging.** Server-side encryption on; object versioning + object lock for the retention window. | Standard answer. Object lock prevents accidental delete; lifecycle rules enforce the cold-tier expiry. Cross-region replication is an opt-in if the legal team requires it. |
| **Cold tier — catalog** | **AWS Glue Data Catalog** (or **Hive Metastore** in non-AWS deploys). | Trino reads partition metadata from a catalog; rolling our own means re-implementing schema evolution. |
| **Federated query layer** | **Trino** (or **ClickHouse external tables** as the bridge / lighter alternative — see "Phasing" below). | Trino runs federated queries across the ClickHouse hot tier and the Parquet cold tier in one SQL surface. Mature, runs in k8s, large ecosystem. |
| **Schema** | **Identical column shape across hot and cold.** The Parquet writer mirrors the existing `MergeTree` columns 1:1 (envelope + payload + nested `sampled_values`). | A query that today reads from ClickHouse should read from the federated view with no rewrite — same column names, same types. Schema drift between tiers is the single biggest source of hot/cold-tier bugs in production. |

### Write-path approach

We sketch three options in the candidate-direction section above
(dual-write from the ingestor; ClickHouse-export of aged
partitions; separate stream processor on Kafka). The **recommended
default is Option B — ClickHouse-driven export of aged partitions**.
Reasoning:

- Single source of truth on the hot side. The ingestor stays
  simple — it keeps writing only to ClickHouse, exactly as today
  (ADR-0020).
- A scheduled job (Kubernetes `CronJob` or Airflow / Dagster
  task, depending on what the platform already runs) selects
  partitions about to age out, exports them to Parquet via
  `INSERT INTO FUNCTION s3(...) SELECT ... FROM cp_meter
  WHERE _partition_id = ...`, verifies the row count round-trips,
  then drops the hot-tier partition. The TTL DELETE is the
  fallback in case the export job is paused — but in normal
  operation, partitions move from hot to cold via the export,
  not via passive expiry.
- "Never lose data" survives because **the hot-tier delete is
  conditional on the cold-tier write succeeding**. The export
  job verifies the Parquet file's row count equals the source
  partition's row count, and only then drops the source.
- Why not Option A (dual-write from ingestor): doubles the
  ingestor's failure surface. Today the ingestor's blast radius
  is one path (Kafka → ClickHouse); adding a second path means
  every row must succeed twice or fail back to Kafka, which
  needs careful idempotency on both sides.
- Why not Option C (separate stream processor): adds a third
  runtime substrate (Flink or similar) that nobody on the team
  runs today. Worth revisiting if a stream-processor is brought
  in for an unrelated reason (e.g. real-time aggregations).

The export-job approach has one real downside: a partition is
**only in the hot tier** between ingest and the export job
running. If ClickHouse loses data in that window before export,
the cold tier doesn't have it. Mitigations:

- The export window is small (days, not weeks) by sizing the
  job's frequency.
- ClickHouse backups (`clickhouse-backup` to S3, landing under
  E5-10) cover the same window from a different angle.
- The Kafka topics are themselves a recoverable source within
  Kafka's retention window (default 7 days) — re-running the
  ingestor against an offset replay reconstructs lost partitions.

These three together are the practical ceiling on data loss.
Document them as the layered defense in the design doc that
upgrades this ADR to Accepted.

### Read-path approach

- **A single Trino "view" per logical table.** The view UNIONs
  the hot-tier ClickHouse table and the cold-tier Parquet table
  with a partition-pruning predicate that routes recent queries
  to ClickHouse and old queries to Parquet. A typical query like
  `WHERE occurred_at >= now() - INTERVAL 7 DAY` hits ClickHouse
  only; `WHERE occurred_at BETWEEN '2027-01-01' AND '2027-03-01'`
  hits Parquet only; a query that spans the boundary fans out
  to both with Trino's federated planner stitching the results.
- **Application code calls Trino, not ClickHouse directly**, for
  any read that might span the retention boundary. The gateway's
  REST timeseries endpoints (`/meter-values`, `/status-history`)
  switch from talking to ClickHouse via the existing
  `ClickHouseReadClient` to talking to Trino via a new client.
  Existing direct-ClickHouse reads stay direct for performance-
  critical paths that are guaranteed in-window.
- **Cache the catalog metadata.** Trino's per-query partition
  discovery against Glue / Hive is the usual performance cliff
  in this design. Configure Trino's metadata cache aggressively
  (minutes-scale TTL) since partitions don't appear or vanish
  outside the export job.

### Phasing — the "bridge" version before full Trino

Standing up Trino is real work. If the trigger conditions fire
before the team has bandwidth for the full design, the **bridge
version** is:

1. Stand up the cold tier first (Parquet + S3 + lifecycle rules).
2. Wire the export job (ClickHouse → Parquet).
3. Add the hot-tier TTL DELETE per the recommended retention
   numbers.
4. **Skip Trino**. Reads stay hot-tier-only; cold-tier access is
   ad-hoc (data team queries Parquet via Athena / DuckDB / a
   notebook) until the federated read pattern becomes load-
   bearing.

This bridge is **70% of the value for 30% of the work**: it
prevents storage cost runaway, satisfies the audit-archive
requirement, and keeps the door open for Trino later. The
decision to add Trino can wait for a real federated-query
demand (an operator UI that shows "this charger over the past
3 years", a billing dispute that crosses the hot/cold boundary,
etc.).

### What the migration looks like, sequenced

When the trigger fires and this ADR upgrades to Accepted:

1. **Cold-tier infra** — S3 bucket, lifecycle rules, IAM, Glue
   catalog. No application changes. ~1 sprint of platform work.
2. **Export job** — read partitions, write Parquet, verify row
   count, drop source. Idempotent (re-running on the same
   partition is a no-op). Run on a small fleet first; verify
   row counts match across both tiers for a week. ~1 sprint.
3. **Hot-tier TTL** — `ALTER TABLE cp_meter MODIFY TTL ...` per
   the recommended numbers. Only flip after step 2 has run
   green for at least one full export cycle. ~1 day.
4. **Trino (or skip per the bridge above)** — federated query
   layer, view definitions, application client switch. Largest
   single piece. ~2 sprints.
5. **Application read switch** — gateway REST endpoints move
   from `ClickHouseReadClient` to a Trino client. Behind a
   feature flag with both paths live, then the flag flips and
   the ClickHouse-direct path is removed. ~1 sprint.

Total: ~5–6 sprints with the full Trino path; ~2–3 sprints for
the bridge. The bridge can later evolve into the full path
without rework — none of the bridge's components are
throw-away.

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
