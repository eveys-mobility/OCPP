# Idempotency and replay

**Audience.** Anyone debugging duplicate messages, designing a consumer that must be idempotent, or understanding why the gateway never double-counts a charging session.

**What this answers.** Why the gateway sees the same OCPP message twice sometimes, how it dedupes those replays, and what that means for *your* code receiving events on the other end.

> Related: [`how-ocpp-flows-work.md`](./how-ocpp-flows-work.md) for the full message lifecycle; [`multi-pod-and-routing.md`](./multi-pod-and-routing.md) for the cross-pod side.

---

## Why duplicates happen at all

OCPP is an at-least-once protocol over a flaky transport. Three sources of duplicates, all benign:

### 1. The charger doesn't trust the network

A charger sends `StopTransaction.req`. The TCP socket dies before the CALLRESULT comes back. The charger has no way to know whether the gateway received it. **Per spec**, the charger retries on reconnect — the message lands again, possibly with the same `message_id`, possibly with a fresh one.

This is normal. A charger that *doesn't* retry would lose every message that crosses a flaky link, and field deployments are full of flaky links.

### 2. The gateway sees a reconnection storm

A pod restarts. Hundreds of chargers reconnect within seconds. Each may replay any messages they had queued during the disconnection — boots, status notifications, even transactions that had started before the disconnect.

### 3. The internal pipeline is at-least-once too

When the gateway publishes to Kafka, the producer retries on failure. A retry could deliver a message the broker had already accepted (the broker's `enable.idempotence` mostly prevents this at the partition level, but a producer failover can still leave the upstream observer seeing duplicates if the broker side wasn't fully idempotent for that partition state).

The combination: a backend consumer might see the same `tx.started` event arrive twice. Once because the charger replayed `StartTransaction`; again because the Kafka producer retried.

---

## What the gateway protects against

The gateway dedupes at two layers, in this order:

### Layer 1: Redis idempotency cache (the hot path)

Every inbound CALL goes through a check:

```python
key = f"idem:{cp_id}:{message_id}"
already_seen = redis.set(key, "1", ex=IDEM_TTL, nx=True) is None
if already_seen:
    metrics.idempotency_lookups_total.labels(outcome="hit").inc()
    return previous_response_from_redis
# ... otherwise: handle normally, cache the response
```

`message_id` is the charger-supplied id from the OCPP wire envelope. A retry of the same message comes through with the **same** `message_id` — the gateway detects this, returns the previously-computed response, and **no side effects fire**: no Postgres write, no Kafka publish, no webhook delivery.

Hit rate is observable via `eveys_ocpp_idempotency_lookups_total{outcome="hit"|"miss"}`. A normal fleet sees a handful of hits per minute — every blip in network connectivity contributes one or two.

TTL is `EVEYS_OCPP_IDEMPOTENCY_TTL_SECONDS` (default a few minutes; long enough to cover charger retry windows, short enough to not pin a stale value forever).

### Layer 2: Postgres natural-key defense

The hot-path cache is a Redis backstop, not a primary key. If Redis is unreachable, or the TTL has expired, the gateway falls through and would try to write a duplicate row.

The persistent tables defend against this with **natural-key uniqueness**:

- `transactions.transaction_id` is unique. A duplicate `StartTransaction` that gets past the Redis cache attempts to insert with a different `id` but the same `transaction_id` — `INSERT ... ON CONFLICT (transaction_id) DO NOTHING` makes it a no-op.
- `transactions.idempotency_key = sha256(cp_id, transaction_id, meter_stop_wh)` is unique. A duplicate `StopTransaction` that lands collides on this — same no-op semantics.
- `security_events` and the migration history have their own append-only / `ON CONFLICT DO NOTHING` shapes.

So even if both Redis layers fail simultaneously (impossible in practice; you'd need Redis *and* the in-process cache *and* the Postgres connection pool to all do something pathological), the database refuses the duplicate row. Counter: `eveys_ocpp_stop_transaction_replays_total` increments when this happens.

---

## What you must do on your side

The gateway prevents duplicate **state mutations**. It does not prevent your consumers from seeing duplicate **events**.

This is on purpose. Kafka is at-least-once. Webhooks are at-least-once. Even if the gateway's hot path is perfectly deduped, the consumer-side delivery isn't:

- A Kafka consumer that crashes between processing a message and committing the offset will re-process it on restart.
- A webhook endpoint that returns a 5xx triggers a retry; if your endpoint actually succeeded but the response got lost, the retry is a duplicate from your view.

Therefore: **your consumers must be idempotent on `event_id`**.

### The pattern that always works

```python
# Pseudocode for any event consumer
def handle_event(envelope):
    event_id = envelope.event_id
    if already_processed(event_id):
        return  # 200 OK; we've seen this one
    with transaction():
        do_the_work(envelope)
        mark_processed(event_id)
```

`already_processed` is typically a small table or Redis set keyed on `event_id` with a TTL longer than the gateway's webhook retry budget. Sub-millisecond check; trivial to implement; bulletproof.

The alternative — making the work itself idempotent (e.g. upserts everywhere) — is fine but doesn't generalise. The `event_id` check is cheap and covers every event regardless of payload shape.

---

## A worked example: a billing service

Suppose your billing service issues invoices from `tx.stopped` events.

Without idempotency:

1. `tx.stopped` arrives for `transaction_id=12345`.
2. Billing creates invoice `INV-001` for the user.
3. Webhook retry arrives (your endpoint took too long to respond).
4. Billing creates *another* invoice `INV-002` for the same transaction.
5. User gets billed twice.

With idempotency:

1. `tx.stopped` arrives for `transaction_id=12345`, `event_id=abc-123`.
2. Billing checks `seen_events`: `abc-123` not there → process. Creates `INV-001`. Writes `abc-123` to `seen_events`.
3. Webhook retry arrives with `event_id=abc-123`.
4. Billing checks `seen_events`: `abc-123` is there → 200 OK, no-op.
5. User gets billed once.

This works regardless of why the duplicate happened — charger retry, gateway retry, webhook retry, consumer crash. The `event_id` is the single anchor.

---

## What `event_id` is, exactly

A UUID generated by the gateway at the moment the event is constructed. **Stable per logical event.** Even if the same `tx.stopped` is published to Kafka three times because of producer retries, all three carry the same `event_id`. The gateway does not generate a fresh `event_id` per delivery attempt.

It's distinct from:

- The OCPP `message_id` (charger-supplied; only meaningful within one CALL).
- Your `request_id` (REST/gRPC trace anchor; one per HTTP request).
- The Kafka offset (transport-level; partition-specific).

Trust `event_id` for deduplication on your side. Don't try to compose a synthetic key from payload fields — `event_id` is the gateway's promise that "this is the same event."

---

## The two events that don't replay

The gateway is conservative about emitting events: a deduped *handler* call doesn't emit. So:

- A replayed `StartTransaction` does **not** produce a second `tx.started` event.
- A replayed `StopTransaction` does **not** produce a second `tx.stopped` event.

The metrics `eveys_ocpp_boot_replays_total` and `eveys_ocpp_stop_transaction_replays_total` count these — useful for diagnosing whether a charger is unusually retry-happy.

This is a stronger guarantee than just consumer-side dedup: the firehose itself is not polluted with replays. Your consumer will still see *retry-induced* duplicates from Kafka/webhooks, but not *protocol-induced* ones.

---

## Edge case: in-flight replays during pod restart

What if the gateway restarts mid-replay? The charger's retry arrives at a brand-new pod. The Redis cache is shared across pods, so the dedup still works. The Postgres natural-key check is also shared. Both layers survive pod churn.

The one place where this could go wrong: if the Redis instance itself is reset (a `FLUSHALL`, or a new Redis brought up cold). The first replay after such an event might slip through Layer 1 and land in Postgres — where Layer 2 still catches it. The gateway logs at warn level when Layer 2 triggers (`replay caught by natural key`), so you'll know.

In normal operation, Layer 2 should be silent. If you see it firing regularly, something is wrong with Layer 1 — usually Redis flapping or a TTL too short.

---

## Summary

| Source of duplicate | Caught by |
|---|---|
| Charger retries a CALL with the same `message_id` | Redis idempotency cache (Layer 1) |
| Charger retries with a different `message_id` but the same `transaction_id` / `meter_stop_wh` | Postgres natural key (Layer 2) |
| Gateway producer retries a Kafka publish | Kafka producer idempotence + your consumer-side `event_id` check |
| Webhook retries because your endpoint flapped | Your consumer-side `event_id` check |
| Consumer crashes after processing but before committing | Your consumer-side `event_id` check |

The gateway handles the upstream half. You handle the downstream half — with one cheap check per event.

---

## Where to go from here

- The full message lifecycle that produces these events: [`how-ocpp-flows-work.md`](./how-ocpp-flows-work.md).
- Why the gateway sometimes needs to route a command to a different pod (and how that interacts with replays): [`multi-pod-and-routing.md`](./multi-pod-and-routing.md).
- How events actually reach your code: [`../guides/consume-events.md`](../guides/consume-events.md).
