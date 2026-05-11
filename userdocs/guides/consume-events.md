# Consume events

**Audience.** A backend or data developer subscribing to charger events.

**What this answers.** Two transports — Kafka and webhooks — one envelope shape. When to pick which. Ordering, idempotency, and at-least-once semantics you can rely on.

> The full event catalogue (every topic, every payload) is [`../reference/events.md`](../reference/events.md). This page is the *integration shape* — how to consume them, not what each one says.

---

## 1. One envelope, two transports

Every event the gateway publishes carries the same protobuf wrapper:

```protobuf
message EventEnvelope {
  string event_id      = 1;   // UUID, unique per event
  string occurred_at   = 2;   // ISO-8601 server-receive timestamp (UTC)
  string cp_id         = 3;   // charger this event is about
  string schema_version = 4;  // "v1"
  string trace_id      = 5;   // optional, for distributed tracing
  oneof payload {
    CpConnected             cp_connected             = 100;
    CpBoot                  cp_boot                  = 101;
    CpStatus                cp_status                = 102;
    CpMeter                 cp_meter                 = 103;
    TxStarted               tx_started               = 104;
    CpSecurityEvent         cp_security_event        = 105;
    TxStopped               tx_stopped               = 106;
    CpDisconnected          cp_disconnected          = 107;
    CpFirmwareStatusChanged cp_firmware_status_changed = 108;
    CpDiagnosticsStatusChanged cp_diagnostics_status_changed = 109;
    CpCsrSubmitted          cp_csr_submitted         = 110;
  }
}
```

Which payload is set is what tells the consumer "what kind of event is this". The wrapper is the same regardless of transport.

The proto file lives at `proto/events/v1/events.proto` in the repository — point your build system at it to generate consumer-side bindings in your language.

---

## 2. When to choose Kafka

Pick Kafka when you want:

- **A firehose** you can replay from any offset. The consumer chooses when to read; the gateway publishes and forgets.
- **Multiple consumers** for the same events without the gateway needing to know about them. New consumers come up, read from `earliest`, and catch up.
- **Strong ordering per charger.** Every event uses `cp_id` as the partition key, so a single charger's events are strictly ordered. Cross-charger ordering is not guaranteed and you should not depend on it.
- **At-least-once delivery** with consumer-side idempotency. Standard Kafka semantics.

Typical use cases: a billing service tailing `tx.stopped`, a fleet-status dashboard tailing `cp.status` and `cp.connected`/`cp.disconnected`, an audit pipeline tailing `cp.security_event`, an analytics warehouse ingesting everything.

### 2.1 Topic names

The defaults are:

| Topic | Payload | Volume |
|---|---|---|
| `cp.connected` | `CpConnected` | per WS connection establish |
| `cp.disconnected` | `CpDisconnected` | per WS connection end |
| `cp.boot` | `CpBoot` | rare; on charger reboot |
| `cp.status` | `CpStatus` | bursts of a few per state change |
| `cp.meter` | `CpMeter` | high — every meter sample on every active session |
| `cp.firmware_status` | `CpFirmwareStatusChanged` | low; firmware-update lifecycle |
| `cp.diagnostics_status` | `CpDiagnosticsStatusChanged` | low; diagnostics-upload lifecycle |
| `cp.security_event` | `CpSecurityEvent` | sparse; audit-grade |
| `cp.csr_submitted` | `CpCsrSubmitted` | sparse; cert-rotation cycles |
| `tx.started` | `TxStarted` | one per charging session start |
| `tx.stopped` | `TxStopped` | one per charging session end |

The names are configurable via `EVEYS_OCPP_KAFKA_TOPIC_*` env vars in case your platform uses a different naming convention.

### 2.2 Consumer example (Python)

```python
from aiokafka import AIOKafkaConsumer
import asyncio
from events.v1 import events_pb2     # your generated bindings

async def main():
    consumer = AIOKafkaConsumer(
        "tx.stopped",
        bootstrap_servers="kafka.example.com:9092",
        group_id="billing-service",
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            env = events_pb2.EventEnvelope()
            env.ParseFromString(msg.value)
            if env.WhichOneof("payload") != "tx_stopped":
                continue
            handle_tx_stopped(env.cp_id, env.tx_stopped)
            await consumer.commit()
    finally:
        await consumer.stop()

asyncio.run(main())
```

`enable_auto_commit=False` plus an explicit commit after handling is the standard "at-least-once with consumer-side idempotency" pattern.

---

## 3. When to choose webhooks

Pick webhooks when:

- **You don't run a Kafka client.** Your backend already accepts HTTPS POSTs and that's the only push mechanism you want to maintain.
- **You want immediate delivery** rather than running a long-lived consumer loop.
- **The event volume is low.** Webhooks are a great fit for `tx.started`, `tx.stopped`, `cp.online`, `cp.offline`, security events. They are a *bad* fit for `cp.meter` and `cp.status` at fleet scale (high frequency × HTTP overhead).

### 3.1 The delivery shape

The gateway POSTs JSON (not protobuf — the wire is HTTP, the payload is the same envelope JSON-encoded):

```http
POST /api/eveys/webhooks/tx.stopped HTTP/1.1
Host: <your-backend>
Content-Type: application/json
X-Eveys-Signature: sha256=<lowercase-hex-digest>
X-Eveys-Event-Type: tx.stopped
X-Eveys-Event-Id: <uuid>
X-Eveys-Delivery-Attempt: 1

{
  "event_id":   "...",
  "occurred_at":"2026-05-11T10:00:00Z",
  "cp_id":      "CP_X",
  "schema_version": "v1",
  "payload": {
    "tx_stopped": { ... }
  }
}
```

### 3.2 Verify the signature

Every delivery is HMAC-SHA256 signed with `EVEYS_OCPP_WEBHOOK_SECRET`:

```python
import hmac, hashlib

def is_valid(body: bytes, header: str, secret: bytes) -> bool:
    expected = "sha256=" + hmac.new(secret, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, header)
```

Reject deliveries with a missing or wrong signature. Use a constant-time comparison (above; `hmac.compare_digest` in Python) to dodge timing attacks.

### 3.3 At-least-once with retries

The gateway treats your endpoint as "successful" on any 2xx. On non-2xx or a network error, it retries with exponential backoff up to a configurable budget. Your endpoint will receive duplicates from time to time — **be idempotent on `event_id`**.

A simple shape: maintain a recent `event_id` cache on your side. First time you see one, process; subsequent deliveries, 200 OK without doing anything.

### 3.4 Configure delivery

In the gateway environment:

```bash
EVEYS_OCPP_WEBHOOK_URL_TX_STOPPED=https://backend.example.com/api/eveys/webhooks/tx.stopped
EVEYS_OCPP_WEBHOOK_ENABLE_TX_STOPPED=true
EVEYS_OCPP_WEBHOOK_SECRET=<32+ bytes of base64>
```

Each event has its own `WEBHOOK_URL_*` and `WEBHOOK_ENABLE_*` pair. Mix and match — subscribe to some events by webhook and some by Kafka, your call.

---

## 4. Both at once?

Yes. The gateway publishes to Kafka and (independently) dispatches webhooks. Subscribing to the same event over both is allowed; you'll see the event arrive twice, once per transport, each carrying the same `event_id`. Idempotency on `event_id` makes that safe.

A common pattern: subscribe to high-volume events (`cp.meter`, `cp.status`) over Kafka and low-volume "I want to know now" events (`tx.started`, `tx.stopped`, `cp.security_event`) over webhooks. Lets the backend keep its synchronous billing logic in the webhook handler while shipping the firehose to a separate analytics consumer.

---

## 5. Ordering — what you can and can't rely on

What you **can** rely on:

- **Per-`cp_id` ordering on Kafka.** All events for one charger arrive in the order the gateway saw them.
- **Per-`cp_id` ordering across event types.** A `tx.started` for charger X arrives before the `cp.meter` samples for the same charger's session.
- **Webhook order is best-effort.** Concurrent retries can deliver out of order.

What you **cannot** rely on:

- **Cross-charger ordering.** `tx.started` for charger X may arrive before or after `tx.stopped` for charger Y — they're on different partitions.
- **Synchronous causality between REST commands and events.** A `RemoteStart` REST call returns `Accepted` independently of the `tx.started` event publishing. Don't block your REST caller on the Kafka offset advancing.

For the deeper picture of what messages flow when, [`../concepts/how-ocpp-flows-work.md`](../concepts/how-ocpp-flows-work.md).

---

## 6. What's not on either transport

A handful of OCPP messages have no dedicated event:

- **Heartbeats** — absorbed by the online registry. The fact that a charger heartbeated is reflected in `last_heartbeat_at` on the REST surface, not as a Kafka message. At fleet scale, "heartbeat received" events would drown everything else.
- **Authorize** — synchronous; the gateway forwards the charger's request to your backend's hot-path REST endpoint. No event because the backend was already in the loop.
- **Outbound command results** (`RemoteStart` reply, `Reset` reply, etc.) — synchronous REST responses; not republished as events.

If a downstream consumer wants any of these, the right hook is the metrics endpoint (`/metrics`) or the REST read endpoints.

---

## Where to go from here

- Full per-event reference: [`../reference/events.md`](../reference/events.md).
- The synchronous side of the same flow: [`use-the-rest-api.md`](./use-the-rest-api.md).
- Why duplicates happen and how the gateway prevents *its own* duplicates: [`../concepts/idempotency-and-replay.md`](../concepts/idempotency-and-replay.md).
