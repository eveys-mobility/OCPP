# ADR-0029: Per-CP offline duration as a gateway-emitted event

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-12 | Mostafa |

## Context

The gateway already emits `cp.connected` and `cp.disconnected` on the
WS lifecycle (ADR-0015 envelope, published in `transport/ws_server.py`).
A downstream consumer that wants to answer "how long was charger X
offline last time" can in principle stitch the two streams: for every
`cp.connected`, find the prior `cp.disconnected` for the same `cp_id`
and subtract.

In practice that's lossy and fragile:

- `cp.connected` and `cp.disconnected` ride different Kafka topics. Per-
  topic ordering is preserved only within a partition; cross-topic
  ordering for the same `cp_id` is not. A late-arriving disconnect from
  a slow producer can land after the next connect, so the join sees a
  spurious zero-second outage.
- Pods scale horizontally. A disconnect observed by pod A and a
  reconnect handled by pod B both publish — the consumer has to
  reconcile across partitions and across producer identities.
- Every consumer that wants the number rewrites the same join. Console
  did; the analytics pipeline would; an external integrator would too.
  Each implementation drifts.

We also have two reader contexts that want the same number:

- The Console list page (`GET /api/v1/charge-points`) wants
  "this CP was offline 4h yesterday" inline, so operators can sort by
  it without a separate fetch.
- An analyst wants the full reconnect-by-reconnect history of one CP
  to debug a flaky station.

## Decision

Emit a new event `cp.offline_duration` from the gateway at reconnect
time, carrying the gap that just closed. Persist it in ClickHouse and
expose two reader surfaces: a paginated history endpoint and a
bulk-batched enrichment on the existing list + detail endpoints. No
Postgres mirror.

Mechanics:

- On disconnect (under the same `was_ours` compare-and-delete gate as
  `cp.disconnected`), the gateway writes a `cp:last_offline_at:{cp_id}`
  hash in Redis with `went_offline_at` / `pod_id` / `reason`. No TTL —
  outages of any length must remain measurable.
- On the next connect, the gateway reads and deletes the marker,
  computes the gap, and publishes `cp.offline_duration` with
  `went_offline_at`, `came_online_at`, `offline_seconds`,
  `prior_pod_id`, `prior_reason`. The publish happens before
  `cp.connected` so stream-order consumers see the closing-window
  event first.
- The ClickHouse ingestor sidecar (ADR-0020) subscribes to the new
  topic and writes rows into `cp_offline_duration` (monthly partition
  on `came_online_at`, order key `(cp_id, came_online_at)`).
- `GET /api/v1/charge-points/{cp_id}/offline-history` queries that
  table with `since` / `until` filters and dual-mode pagination
  matching the `/charge-points` convention.
- `GET /api/v1/charge-points` and `GET /api/v1/charge-points/{cp_id}`
  call a bulk `fetch_latest_offline_durations(cp_ids)` ClickHouse
  helper alongside the existing `fetch_latest_connector_statuses` —
  one round-trip per page, populating `last_offline_seconds` and
  `last_offline_ended_at`.

## Alternatives considered

- **Stitch in the consumer** — keep emitting only `cp.connected` /
  `cp.disconnected`, document the join. **Why rejected**: every
  consumer re-implements the same fragile cross-topic reconcile, and
  the cross-pod / out-of-order cases below would have to be re-
  diagnosed each time. The gateway already has the full state to
  compute the gap once, authoritatively.

- **Postgres column on `charge_points`** — store
  `last_offline_seconds` and `last_offline_ended_at` directly on the
  charger row, update at reconnect. **Why rejected**: Postgres is the
  identity + latest-wins store in this stack (ADR-0004 / ADR-0020).
  Mirroring a per-event timestamp there means dual-write (PG + CH)
  with no clean way to keep them consistent on a CH replay. The CP
  list endpoint already enriches from ClickHouse for `connectors[]`;
  one more bulk lookup is the same shape and no extra tier.

- **Postgres-only event table `cp_offline_periods`** — append-only
  table inside Postgres. **Why rejected**: append-only streams of
  unbounded rows belong in the time-series tier. Postgres would
  acquire monthly partition maintenance it has nowhere else, plus
  vacuum pressure on a hot insert path. The existing analytics tier
  is built for this exact shape.

- **Compute on read from `cp_status` / connect/disconnect events** —
  derive the gap with a ClickHouse query against existing tables on
  every reader hit. **Why rejected**: same cross-topic ordering
  hazard, plus a much heavier scan than `argMax` on a dedicated
  table. The write-side cost of a tiny extra row at reconnect is
  trivially smaller than the read-side cost of recomputing per
  request.

- **Background sweeper that emits durations from Redis TTL expiry** —
  use Redis keyspace notifications to detect a disappearing online
  key and synthesize a `cp.offline_duration`. **Why rejected**:
  keyspace notifications are best-effort in Redis and would couple
  this feature to a Redis configuration flag operators have to
  remember to set. Gateway-side at-reconnect computation needs no
  Redis-side feature flag and stays in one process.

## Consequences

### Positive

- One canonical answer to "how long was the CP offline" — gateway-
  computed and gateway-emitted, not stitched downstream.
- Console list + detail get a low-latency inline field via the same
  batched-CH-lookup pattern already in use for connector status; no
  N+1 fans at render time.
- The dedicated CH table is partition-aligned with the close-side
  timestamp, so retention drops are predictable and queries by date
  range page well.

### Negative / costs

- A new Kafka topic (one more topic to operate, monitor, retain).
  Volume is bounded by reconnect rate, not message rate — orders of
  magnitude smaller than `cp.meter`.
- A new ClickHouse table to migrate and back up.
- A new envelope variant on the frozen v1 oneof (allowed by ADR-0015
  evolution rules — additive only — but it widens the surface).

### Risks

- **Marker write failure leaves the next connect with no gap to
  compute.** Mitigated: best-effort write, logged warning. Worst case
  is under-reporting one outage, never a 500 or a fabricated number.
- **Pod crash between disconnect and the next connect skips the
  marker write.** Same: the next reconnect emits nothing. Operators
  see this as "outages with no duration row" in the table; downstream
  alerting can flag it if it becomes common.
- **Cross-pod clock skew** makes `came_online_at - went_offline_at`
  go negative. The publisher emits the negative value rather than
  silently clamping; downstream can filter `offline_seconds >= 0` if
  needed. Time is server-receive on both ends, so this is a real
  cluster-health signal, not a calculation bug.
- **Schema evolution risk on the marker hash.** A marker written by
  an older binary lacking `reason` is read by a newer one — we
  forward an empty string and log. The publisher tolerates missing
  fields.

### Reversibility

- Reversible. The feature is purely additive: a new topic, a new
  table, a new endpoint, two new optional fields on existing
  responses. Disabling means stop subscribing the ingestor to the
  topic and remove the publisher block in `_on_connect`. Existing
  consumers of `cp.connected` / `cp.disconnected` are untouched.
- The Redis marker has no TTL but is single-shot per outage, so it
  cannot accumulate beyond one key per offline CP at any moment.

## References

- ADR-0004 — ClickHouse as the time-series store
- ADR-0015 — Kafka event envelope format (v1 oneof evolution rules)
- ADR-0020 — ClickHouse ingestion sidecar
- ADR-0026 — Gateway REST API conventions (pagination, error shapes)
- Issue #210 — feature request and acceptance criteria
- PR #211 — implementation
