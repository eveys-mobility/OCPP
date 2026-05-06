# ADR-0019 — Kafka producer hardening: durability over throughput

- **Status**: Accepted
- **Date**: 2026-05-01
- **Author**: Eveys engineering (E2-7; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

E2-1 shipped a `KafkaEventProducer` running aiokafka defaults (`acks=1`, no idempotent producer, `linger_ms=0`). It was good enough for the `MeterValues` firehose: lost meter samples are an analytics inconvenience, not a financial event. E2-8 then added three more topics — `cp.boot`, `cp.status`, `tx.started` — and Phase 3 will wire `tx.started` into the session/billing path.

The defaults stop being acceptable at that point. With `acks=1` (leader-only acknowledgment), the producer can receive a successful `send_and_wait` response for a record that was never replicated, then lose it when the leader dies before the follower catches up. **A `tx.started` event lost this way is a session/billing record we silently dropped.**

The fix is straightforward — flip the durability and idempotence flags — but each flag costs latency or throughput, and the trade-offs are real enough to be worth recording.

Forces:

- A `tx.started` lost is a customer-visible billing discrepancy. `cp.meter` lost is an analytics gap. Tolerance for loss differs by topic but the producer is a single shared object.
- Phase 3 (auth/session/device wiring) is gated on a producer that won't drop these events. Tuning now keeps E2-7 off the Phase-3 critical path.
- Any change here lands before real production traffic, so we can pick conservative defaults without an A/B comparison.
- aiokafka's API is fixed: `linger_ms` is producer-wide (not per-`send`), `max_in_flight_requests_per_connection` is **not exposed** as a kwarg (aiokafka sets it internally to 5 when `enable_idempotence=True`).
- Settings should make every value tunable via env so a future operational tweak doesn't require a code change.

## Decision

Flip three durability/idempotence knobs on `KafkaEventProducer.start()`:

```python
AIOKafkaProducer(
    bootstrap_servers=...,
    client_id="eveys-ocpp",
    acks="all",                       # full ISR ack
    enable_idempotence=True,          # producer-side dedup on retry
    linger_ms=5,                      # see "Per-topic linger" below
    request_timeout_ms=30_000,
    retry_backoff_ms=200,
)
```

All five new kwargs are exposed as `Settings` fields with the values above as defaults, range-bounded sensibly. They can be overridden via `EVEYS_OCPP_KAFKA_ACKS`, `EVEYS_OCPP_KAFKA_ENABLE_IDEMPOTENCE`, `EVEYS_OCPP_KAFKA_LINGER_MS`, `EVEYS_OCPP_KAFKA_REQUEST_TIMEOUT_MS`, `EVEYS_OCPP_KAFKA_RETRY_BACKOFF_MS`.

### Per-topic linger: single producer, compromise value

The originally proposed shape was `linger_ms=20` for `cp.meter` (high volume, batching gains) and `linger_ms=0` for the other three topics (low volume, latency wins). aiokafka doesn't support per-call linger and we don't want to maintain two producer instances for marginal gain. Compromise: **single producer, `linger_ms=5`**.

Math:
- `cp.meter` at 10k chargers / 30s reporting = ~333 msg/s. 5ms linger batches comfortably; we lose maybe 30% of the throughput headroom available with `linger_ms=20`, which we don't currently need.
- `cp.boot`, `cp.status`, `tx.started` are low-volume. A 5ms latency floor is invisible vs the OCPP RTT (tens of ms over WS).

If a future load test shows we're hitting a `cp.meter` ceiling, splitting into two producers is easy and reversible. Don't pre-optimize.

### `max_in_flight_requests_per_connection` is not configurable

aiokafka does not expose this. With `enable_idempotence=True` the library hardcodes the upper bound to 5 (Kafka protocol requirement to maintain ordering across retries). The cap is enforced for us; we don't pass it explicitly.

### Reconnect / retry behaviour

We rely on aiokafka's built-in reconnect on broker drop. The two knobs that matter:

- `request_timeout_ms` — how long a single produce request waits for the broker. Default 40 s; we tighten to 30 s so a stuck broker fails the call before the OCPP handler's own ceilings start cascading. (The handler-level `try/except` from E2-8 catches the failure and logs `<handler>.publish_failed`; the WS reply still goes back to the charger.)
- `retry_backoff_ms` — how long aiokafka waits between retries on a recoverable error. Default 100 ms; we set 200 ms to slow a retry storm against a flailing broker without making one-off transient errors painful.

Idempotent producer + `acks=all` means aiokafka's own retry on a transient error is safe (no duplicates). The handler's try/except guard from E2-8 is the second line of defense for the truly-broken case.

## Alternatives considered

- **Keep `acks=1`** (current state). Cheaper publish latency, but loses durability on leader crash. Rejected: `tx.started` is on the financial path; no acceptable defense for "we acked it then lost it".

- **`acks=0`** (no ack at all, fire-and-forget). Considered for `cp.meter` only. Rejected: we don't have per-topic acks any more than we have per-topic linger, and the loss tolerance on the other three topics doesn't allow it.

- **Two producers** (one for `cp.meter`, one for the rest), each with its own linger. Rejected for now: doubles the resource footprint (two TCP connections, two metadata fetches, two reconnect state machines) for a throughput optimization we don't currently need. Reversible — splitting later is one constructor call away.

- **`linger_ms=0`** everywhere. Lowest latency. Rejected: drops `cp.meter`'s batching gains and ~doubles the connection-level message rate at fleet scale. 5 ms is a cheap compromise.

- **`linger_ms=20`** everywhere. Best throughput. Rejected: `tx.started` and `cp.boot` shouldn't sit in a 20ms batch. Phase 3 consumers will read these, and 20ms latency floor on every billing-relevant event is unnecessary tax.

- **Transactional producer** (`transactional_id` set, `init_transactions`, `begin_transaction`, `commit_transaction`). True exactly-once across producer crashes. Rejected for now: it's a much bigger lift (the handler call sites need transaction begin/commit/abort wrapping; a partial commit on broker error becomes a recovery scenario), and idempotent-producer + `acks=all` already gets us the no-duplicates property within a single producer session. Revisit if a load test or staging incident shows we need cross-session exactly-once.

- **Compression (`compression_type='zstd'`)**. Real saving on the wire; aiokafka supports it. Held back — it interacts with broker config (the broker must accept the compression type) and with consumer-side decompression in unknown downstream services. Worth a follow-up MR with a load-test datapoint, not a default flip in this ADR.

## Consequences

### Positive

- `tx.started`, `cp.boot`, `cp.status`, `cp.meter` events are now durable to a Kafka leader crash. Phase 3's session/billing wiring inherits this for free.
- Idempotent producer eliminates duplicate-event-on-retry — pairs cleanly with E2-11's inbound-replay dedup and removes another source of double-counting.
- All five kwargs are env-tunable: an ops team can lower `linger_ms` if a single charger's latency budget gets tight without a code change.
- aiokafka's reconnect/retry loop already exists; we're tuning, not adding code.

### Negative / costs

- **Publish latency p50** rises ~5–10 ms (`acks=all` round-trip to ISR). Imperceptible in OCPP WS RTT terms but real.
- **Throughput ceiling** drops ~5% from the idempotent-producer overhead. We're nowhere near the ceiling today; flagging for awareness.
- **Compromise linger** loses both extremes: not the lowest-latency, not the highest-throughput. Acceptable until a load test argues otherwise.
- Five new env vars to monitor and document. Each one is a knob someone might fiddle without reading the ADR.

### Risks

- **`acks=all` + ISR=1 in dev cluster**. Single-broker dev/test clusters effectively give you `acks=1` even with `acks=all`. Fine for tests; flagging because a production-like soak needs ISR≥2 to actually exercise this ADR's behaviour.
- **`enable_idempotence=True` requires Kafka broker ≥ 0.11.** All supported brokers (Confluent, MSK, Apache 3.x in our compose stack) are fine; flagging because a future managed-Kafka choice needs to verify.
- **`request_timeout_ms=30_000` is shorter than aiokafka's default 40 s.** If broker latency genuinely spikes past 30 s (e.g. ISR replication stalls), we'll see more publish failures and trip more `<handler>.publish_failed` logs. That's the intended signal — if it happens we want to know — but it can be surprising on first observation. Operators should expect the new value.
- **No on-publish-failure metric yet.** The E2-8 try/except logs `<handler>.publish_failed` but doesn't increment a counter. If the rate of these climbs, we'd want to alert. Tracked in Phase 4 (E4-1, Prometheus metrics).

### Reversibility

- Reversible by env. Set `EVEYS_OCPP_KAFKA_ACKS=1`, `EVEYS_OCPP_KAFKA_ENABLE_IDEMPOTENCE=false`, `EVEYS_OCPP_KAFKA_LINGER_MS=0` to revert to pre-E2-7 behaviour without a code change.
- Code-level reversibility (drop the kwargs from `KafkaEventProducer`) is also trivial.
- Switching to a transactional producer or to two-producer-per-topic-class is a separate ADR; this ADR doesn't lock those out.

## Project conventions implied by this decision

- Every Kafka producer kwarg we tune lives in `Settings` with a default + a description + a range bound.
- A future change to `acks`, `enable_idempotence`, or `linger_ms` (away from the values in this ADR) is a tech-lead-approved decision: amend this ADR, don't drift via env vars alone.
- A handler that needs different durability/latency from the firehose default opens its own producer instance rather than mutating the shared one. (No such handler today.)

## References

- [`src/eveys_ocpp/events.py`](../../src/eveys_ocpp/events.py) — `KafkaEventProducer` implementation.
- [`docs/02-tasks.md`](../02-tasks.md) E2-7.
- [`aiokafka` producer docs](https://aiokafka.readthedocs.io/en/stable/producer.html) — kwarg reference and idempotent-producer notes.
- Kafka protocol — idempotent-producer requirements: `enable_idempotence=True` requires `max.in.flight.requests.per.connection<=5`, `acks=all`, `retries>0`. aiokafka enforces these for us.
- [ADR-0015](./0015-kafka-event-envelope-format.md) — the wire format these tuning knobs apply to.
- [ADR-0017](./0017-idempotency-cache.md) — the inbound-replay dedup that pairs with idempotent producer for end-to-end exactly-once-effect.
