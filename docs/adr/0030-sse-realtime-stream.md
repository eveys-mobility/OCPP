# ADR-0030: Server-Sent Events for the per-CP detail page

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-12 | Mostafa |

## Context

`GET /api/v1/charge-points/{cp_id}` and the per-CP transactions /
meter-values / status-history endpoints are plain GETs. An operator
UI that wants live updates — a new transaction row appearing, an
SoC reading changing, a CP going offline — has to poll.

The current Console behaviour is:

- List page polls `/charge-points?…` at ~30s. Fine for "is anything
  on fire" inventory glance.
- Detail page does not poll. Operators see "stale" data until they
  refresh manually. The user reported: "Transactions history did not
  show it in realtime, reload page is required for see new
  transaction."

The gateway already produces every relevant event on Kafka
(`tx.started`, `cp.meter`, `cp.status`, `cp.disconnected`, etc.,
ADR-0015 envelope), keyed on `cp_id`. The data is there; we just
don't expose a way to subscribe.

## Decision

Add `GET /api/v1/charge-points/{cp_id}/events` — a Server-Sent
Events stream that fans out the Kafka events for one CP to the
caller as `text/event-stream`. One stream per CP. Feature-flagged
off by default.

Mechanics:

- A singleton ``SseBus`` per gateway pod owns one
  ``AIOKafkaConsumer`` subscribed to every topic whose envelope
  carries a ``cp_id`` (connected, disconnected, offline_duration,
  boot, status, meter, firmware_status, diagnostics_status,
  tx_started, tx_stopped, security_event).
- Each open SSE response registers a bounded ``asyncio.Queue``
  (``EVEYS_OCPP_SSE_QUEUE_MAX_SIZE``, default 256). The bus reads
  Kafka, projects the envelope into a JSON dict, and ``put_nowait``s
  it into every queue subscribed to the matching ``cp_id``.
- ``put_nowait`` failure (queue full) marks the subscriber dropped
  and wakes it with a ``None`` sentinel. The endpoint reads the
  sentinel and closes the stream with a terminal ``event: error``.
- Consumer group id is non-durable
  (``<prefix>-<pod_id>-<uuid>``) with
  ``auto_offset_reset='latest'``. SSE tails from now; replay history
  comes from the existing GET endpoints.
- Endpoint emits a ``: heartbeat\n\n`` comment every
  ``sse_heartbeat_seconds`` so intermediate proxies don't close idle
  streams.

Payload shape: each SSE event carries the envelope's common fields
(``event_id``, ``occurred_at``, ``cp_id``, ``schema_version``) plus
the proto payload projected into the same JSON shape the existing
REST endpoints publish. Console reuses its existing decoders.

## Alternatives considered

- **Console polls at 5–10 s on the detail page.** *Why rejected:* it
  fixes the symptom but every operator tab generates a steady RPS,
  much of which returns the same payload. Detail screens that show
  charging energy want second-grade freshness; 5–10 s polls produce
  the "jumpy" feel operators reported. SSE costs one open connection
  per tab and one Kafka consumer per pod, which is a better trade
  for the Console's actual access pattern.

- **WebSocket reverse channel multiplexed across CPs.** *Why
  rejected:* solves a problem we don't have. The Console renders one
  CP per detail page, so per-CP streams are a natural fit. A WS
  multiplexer adds framing, subscription messages, and a heartbeat
  protocol; for a few hundred concurrent operator tabs SSE is
  simpler and works through every proxy without custom config.

- **ClickHouse Kafka Engine table + a polling endpoint.** *Why
  rejected:* the engine table already exists for the ingestor's
  history surface, but querying it on every poll trades Kafka-stream
  bytes for ClickHouse scan bytes. The bus reads Kafka directly,
  bypassing CH; the bus drops a slow consumer without affecting the
  pod's Postgres / CH pools.

- **Cron-style push from the backend.** *Why rejected:* the backend
  doesn't see Kafka envelopes; routing through it would mean a
  double hop (gateway → backend → Console) for data that already
  lives on the gateway pod.

- **Long polling.** *Why rejected:* it's strictly worse than SSE for
  the same shape — open one connection per event, instead of one
  per page lifetime. Higher round-trip overhead, no protocol-level
  framing, awkward to consume in JavaScript.

## Consequences

### Positive

- Detail page sees `tx_started`, `cp.meter`, `tx_stopped` etc.
  within a second of the event landing in Kafka, no polling.
- Same JSON shapes as the existing REST endpoints — no new decoder
  on the Console.
- Per-pod fan-out: any Console session attached to any gateway pod
  sees the full event stream for any CP it subscribes to (every pod
  subscribed to the topic sees every event).

### Negative / costs

- One ``AIOKafkaConsumer`` per pod when the feature flag is on. Small
  in absolute terms (one consumer-group member per pod), but it
  joins a non-durable group that gets new ids on every restart, so
  Kafka sees more brief join/leave traffic than the existing durable
  ingestor consumer.
- Per-subscriber bounded queues (256 dicts per subscriber by
  default). At 50 concurrent operator tabs that's ~12k dict objects
  in flight worst-case — bounded, not a leak, but worth knowing.

### Risks

- **Slow operator → drop.** A Console tab that pauses (browser
  backgrounded with no `requestIdleCallback`) can fall behind and
  get dropped. Mitigation: ``server_closed`` / ``slow_consumer``
  reason on the terminal event tells the client to reconnect; the
  endpoint resubscribes them from "now."
- **Pod-side memory growth on subscribed churn.** Subscribers
  attach/detach as Console tabs open and close. Each open queue is
  bounded, but a misbehaving client that opens many streams and
  never closes could fan out memory. Mitigation: any HTTP idle
  timeout (Envoy, ALB) eventually closes the connection and the
  endpoint's ``finally`` block detaches the subscriber.
- **Auth bypass.** SSE goes through the same bearer-token middleware
  as the rest of `/api/v1/*`. The middleware runs before the route
  handler; an unauthenticated request 401s without ever opening the
  stream.
- **Backpressure into the Kafka consumer.** ``put_nowait`` is the
  whole reason the consumer never blocks on a single slow
  subscriber. If every subscriber for a given ``cp_id`` is slow,
  ``_fan_out`` still returns in O(subscribers) and the consumer
  keeps reading.

### Reversibility

- Reversible. Set ``EVEYS_OCPP_SSE_ENABLED=false`` on every pod and
  the bus is never instantiated; the route is never mounted; the
  consumer never opens. No data is persisted, no schema is created.
- The non-durable group id means tearing the feature out leaves no
  orphan offsets in Kafka.

## References

- ADR-0015 — Kafka event envelope format
- ADR-0019 — Kafka producer hardening
- ADR-0020 — ClickHouse ingestion sidecar
- ADR-0026 — Gateway REST API conventions
- ADR-0029 — Per-CP offline duration as a gateway-emitted event
- Issue #214 — feature request
- PR #215 — implementation
