# ADR-0027: Outbound webhook delivery — Kafka-tail, HMAC, exponential retry

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-07 | Mostafa |

## Context

`docs/integration/03-webhooks.md` froze the gateway-to-backend webhook
contract: the gateway POSTs signed JSON envelopes to backend-configured
URLs whenever events of interest occur. The same events flow on Kafka
already; webhooks are the alternative for backends that don't
subscribe to Kafka directly.

The contract was specced. This ADR records how the delivery
implementation hangs together — choices that don't appear in the
external contract but matter for operators and future contributors.

Three questions had multiple plausible answers:

1. **Where does the dispatcher get its events from?** Subscribe to
   the same Kafka topics the rest of the gateway publishes on, or
   maintain a separate "outbox" table in Postgres that the OCPP
   handlers write to alongside their Kafka emit?

2. **What signs deliveries?** HMAC-SHA-256 (single shared secret) or
   a JWT with short-lived signing keys?

3. **Where does retry state live?** In-memory (lose retries on
   restart), Redis (cluster-shared), or a Postgres `webhook_attempts`
   table (durable, queryable, slower)?

Each picks the simpler answer. The reasoning matters because the
"obvious" choice in each is the durable / cross-instance / observable
one — and we're explicitly choosing the lighter option until volume
justifies the heavier.

## Decision

### 1. Tail Kafka, no separate outbox.

The dispatcher subscribes to the four event topics
(`cp.boot`, `cp.status`, `cp.meter`, `tx.started`) using a distinct
consumer group from the ClickHouse ingestor. Each event published to
Kafka triggers up to one webhook delivery attempt per enabled
subscription URL.

**Why not an outbox table:**
- Kafka is already the durable, replayable source of truth for these
  events. Adding a Postgres queue is a second copy of the same
  facts.
- Outbox-pattern usually exists to handle "what if Kafka's
  unavailable when the handler runs"? In our case the handler
  *requires* Kafka to be up to publish at all — the event never
  reaches webhooks if Kafka was down, but it never reaches anyone
  else either, so the failure mode is symmetric.
- A consumer-group offset is exactly the bookmark we need; reusing
  the existing offset commit machinery is free.

**Trade-off accepted:** webhook deliveries can fall arbitrarily far
behind the live event stream if the backend is unreachable. The lag
shows up as Kafka consumer lag, which our existing alerting can
already see. Operators can intervene by filtering / dropping a topic
in compose, or by deleting + recreating the consumer group to
fast-forward past stuck events.

### 2. HMAC-SHA-256, single shared secret.

Each delivery's body is signed with HMAC-SHA-256 using the secret in
`webhook_secret`. The receiver verifies with the same secret.

**Why not JWT:**
- HMAC body signing protects the contents of the request — exactly
  what the receiver needs to verify. JWT in `Authorization:
  Bearer ...` only proves the issuer; the body is still
  unauthenticated unless the JWT also signs a body hash, at which
  point we're doing HMAC-the-hard-way.
- HMAC requires neither a CA nor a key registry. Rotation is a
  coordinated update of one string in two places.
- The backend is a single trusted party; we don't need
  audience / issuer / scope claims.

**Trade-off accepted:** rotating the secret requires a synchronized
update across gateway and backend. A leak invalidates every signed
delivery in flight; the operator's playbook is "bump the secret, all
in-flight retries fail with 401, both sides accept the loss."

### 3. In-process retry only — no durable retry state.

A failed delivery retries inside the dispatcher process per the
backoff schedule (1s, 5s, 30s, 2min, 10min — five total attempts).
While a delivery is between attempts, it's an in-flight task in the
event loop. If the dispatcher restarts, in-flight retries are lost.
Kafka consumer offset is committed only after the attempt sequence
completes (success OR exhausted), so a restart re-delivers the
batch — at-least-once semantics, but the per-delivery in-flight
state is ephemeral.

**Why not a `webhook_attempts` Postgres table:**
- The ephemeral case is the one we observe: the dispatcher restarts
  rarely (only on deploy), retries finish in 10 minutes worst-case
  (well under most deploy intervals), and a restart mid-retry just
  replays the event from Kafka.
- A durable retry table adds: a new schema, an Alembic migration, a
  reaper for dead-letter rows, a polling loop separate from the
  Kafka loop, and a slow-query risk on the operator dashboard.
  Net: maybe four new files and an ongoing operational burden.
- We can add the table later if a real incident demands it. The
  dispatcher's external API (Kafka in, HTTP out) is the same shape
  either way.

**Trade-off accepted:** if the backend has a 4-hour outage and the
gateway restarts during it, every event from the outage replays once
the gateway comes back. The receiver MUST be idempotent on
`X-Eveys-Event-Id` per the contract anyway, so the duplicate is a
wash. If the backend is down longer than the Kafka retention window
(default 7 days), events older than that are gone — same as for any
other Kafka consumer.

## Alternatives considered

- **Per-event subscription model in Postgres** (multiple URLs per
  event type). Would let the same gateway push to multiple backend
  receivers. Rejected for v0: only one backend exists, only one URL
  per event type, and the contract spec explicitly fixes one URL per
  event. Promote to subscription table if a future ADR adds a
  multi-backend deployment.

- **Webhook signing with mTLS** (no HMAC; trust the TLS chain). Real
  alternative for production. Rejected for now because (a) the
  gateway doesn't terminate TLS — Envoy does — so the gateway can't
  see client certs without extra plumbing; (b) HMAC has fewer moving
  parts and works the same in dev, staging, and prod.

- **Push to Kafka with a "webhook" topic, separate worker**. Adds a
  hop. The dispatcher already runs in-process beside the gateway;
  splitting it out gains nothing until we want a different
  scaling shape (we don't).

- **Use the existing ClickHouse ingestor's consumer group**. The two
  consumers compete for partition assignments → either ClickHouse
  ingestion stalls during webhook backoff, or webhooks fall behind
  ClickHouse. Distinct groups isolate the two loops.

## Consequences

### Positive

- One place to look for delivery state: the dispatcher's structured
  log + Kafka consumer-group lag. Operators don't grow a new dashboard.
- No new database tables, no new Alembic migrations, no new
  reapers. The webhook subsystem is one Python package.
- HMAC verification is 4 lines of receiver code. Backend's
  integration cost is negligible.
- The system degrades predictably: backend down → Kafka lag grows →
  alerts fire on the existing consumer-lag SLO.

### Negative / costs

- Lost retries on dispatcher restart. Mitigated by Kafka replay; the
  receiver MUST already be idempotent on `event_id`.
- Single shared secret rotation requires a coordinated change across
  gateway and backend. Mitigated by short rotation playbook.
- No "give me everything that failed" query; we'd have to grep
  structured logs. Acceptable until we have a real incident.

### Risks

- **Backend outage exceeding Kafka retention.** Default retention is
  7 days. A backend outage longer than that loses events. Mitigation:
  monitor retention vs backend SLO; bump retention if the SLO is
  weeks. (Real risk for `cp.meter` if it's ever turned on, much less
  for the low-volume topics.)
- **Per-charger ordering not guaranteed.** Two events from the same
  charger can deliver out of order if attempt 1 fails and attempt 2
  for a later event succeeds first. Receiver MUST order on
  `occurred_at`, not arrival time. Documented in
  `docs/integration/03-webhooks.md` § Delivery semantics.
- **`cp.meter` saturation.** At 10k chargers @ 1 sample / 30 s, the
  dispatcher would post ~333 webhooks/sec. Default-off; operators
  who turn it on should also bump httpx pool size. Documented in
  the setting's `impact` field.

### Reversibility

Reversible at any layer:

- Switching to an outbox table is additive — new table + new
  consumer; the existing Kafka consumer becomes the writer for it.
- Switching to JWT is a header-shape change in the dispatcher and
  receiver. The body wire format doesn't move.
- Adding durable retry state means writing each `webhook.delivery_*`
  log line into a Postgres table on the way out. The dispatcher's
  retry loop doesn't change.

## Project conventions implied by this decision

- Webhook event types map 1:1 to existing Kafka payload variants.
  Adding a webhook event = adding a Kafka payload variant first
  (proto change + producer + ADR follow-up if it's a new domain
  concept).
- The dispatcher's structured-logging events
  (`webhook.delivered`, `webhook.delivery_attempt_failed`,
  `webhook.delivery_rejected`, `webhook.delivery_failed`) are the
  contract for ops dashboards. New event names need updates in
  `docs/04-contributing.md` and the alerting runbook.
- One URL per event type. Multi-receiver fan-out goes through a new
  ADR.

## References

- `docs/integration/03-webhooks.md` — the contract this ADR
  implements.
- ADR-0015 — Kafka event envelope format. The dispatcher tails
  envelopes, doesn't define them.
- ADR-0019 — Kafka producer hardening. The producer's durability
  guarantees are why we trust Kafka as the durable layer.
- ADR-0020 — ClickHouse ingestor sidecar. Same Kafka-tail pattern,
  different sink. Distinct consumer group.
- E3-9 in `docs/02-tasks.md`.
