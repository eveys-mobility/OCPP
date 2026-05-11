# Architecture at a glance

**Audience.** Anyone deciding whether to integrate or operate this gateway.

**What this answers.** One diagram, one page. The four surfaces, what owns what, and where state lives.

---

## The one diagram

```
                      Chargers (1 ... N)
                              │
                              │ WSS / OCPP 1.6-J  (or 2.0.1)
                              ▼
                    ┌──────────────────┐
                    │      Envoy       │   TLS termination
                    │   ring-hash by   │   Sticky routing on cp_id
                    │       cp_id      │
                    └────────┬─────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
                ▼            ▼            ▼
        ┌────────────┐ ┌────────────┐ ┌────────────┐
        │  Gateway   │ │  Gateway   │ │  Gateway   │     N pods, stateless
        │   pod #1   │ │   pod #2   │ │   pod #N   │     Each owns ~few-K sockets
        └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
              │              │              │
              │              │              │
       ┌──────┴──────────────┴──────────────┴──────┐
       │                                            │
       ▼                                            ▼
  ┌─────────────────┐                  ┌──────────────────────┐
  │ Shared state    │                  │  Event distribution  │
  │                 │                  │                      │
  │  Postgres       │ ◄── relational   │  Kafka topics        │ ──► Your consumers
  │  Redis          │     state        │  Webhook deliveries  │ ──► Your endpoints
  │  ClickHouse     │ ◄── time-series  │                      │
  └─────────────────┘                  └──────────────────────┘

         ▲                                       ▲
         │                                       │
         │  REST  / gRPC                         │
         │  (commands, queries)                  │
         │                                       │
         └──────── Your platform ────────────────┘
```

That's everything. Below is what each part of it is for.

---

## The four surfaces

The gateway exposes — and consumes — four interfaces. Pick the one that matches your role.

### 1. WebSocket (charger-facing, inbound)

- **Who:** every EV charger in your fleet.
- **Protocol:** OCPP 1.6-J or OCPP 2.0.1 over WebSocket Secure.
- **Auth:** per-charger Basic Auth credentials and/or client mTLS at the Envoy edge.
- **Direction:** bidirectional. Chargers initiate `BootNotification`, `Heartbeat`, `StatusNotification`, `Authorize`, `StartTransaction`, `MeterValues`, `StopTransaction`, `DataTransfer`, security events, firmware/diagnostics status, CSR submissions. The gateway initiates `RemoteStartTransaction`, `RemoteStopTransaction`, `Reset`, `UnlockConnector`, `ChangeAvailability`, `ChangeConfiguration`, `GetConfiguration`, `ClearCache`, `TriggerMessage`, smart-charging profile operations, local-list updates, reservations, certificate install/delete, firmware updates, log retrieval, and `DataTransfer`.
- **Hostport in dev:** `ws://localhost:19000/<cp_id>`. In production it's `wss://...` behind Envoy.

You don't talk to this surface unless you're a charger.

### 2. REST + gRPC (platform-facing, outbound from your code)

- **Who:** your backend service, your operations dashboard, your scripts.
- **Protocol:** HTTP/1.1 (REST) on port `8080`, HTTP/2 (gRPC) on port `50051`. Both speak the same operations; gRPC is lower overhead, REST is more familiar.
- **Auth:** bearer token in `Authorization: Bearer <token>`. Tokens are a CSV in the gateway's config.
- **Direction:** request/response. Your code calls; the gateway answers.
- **What's on it:** list chargers, fetch one charger, list transactions, fetch transactions with telemetry, list / approve / reject pending CSRs, query meter-value time series, query status history — plus every OCPP-initiated command above wrapped as a `POST .../commands/<verb>`.

Full surface in [`reference/rest-api.md`](./reference/rest-api.md) and [`reference/grpc-api.md`](./reference/grpc-api.md).

### 3. Kafka (events firehose, outbound from gateway)

- **Who:** any number of consumers in your platform.
- **Protocol:** Kafka. Payload is a versioned protobuf `EventEnvelope` carrying a `oneof payload`.
- **Direction:** the gateway publishes; you consume.
- **Topics in scope today:** `cp.connected`, `cp.disconnected`, `cp.boot`, `cp.status`, `cp.meter`, `cp.firmware_status`, `cp.diagnostics_status`, `cp.security_event`, `cp.csr_submitted`, `tx.started`, `tx.stopped`.
- **Partition key:** every record uses `cp_id`. Per-charger ordering is preserved; cross-charger ordering is not.

Topics and envelope shapes in [`reference/events.md`](./reference/events.md).

### 4. Webhooks (events for backends that prefer push, outbound from gateway)

- **Who:** your backend, if you'd rather receive HTTP POSTs than tail Kafka.
- **Protocol:** HTTPS POST with an HMAC signature header.
- **Direction:** the gateway calls your endpoints.
- **Delivery:** at-least-once, with exponential backoff and a configurable retry budget. Same envelope as Kafka; same set of events.

Webhooks and Kafka are not exclusive — you can subscribe to one event by webhook and another by Kafka.

---

## Where state lives

The gateway is stateless. Every fact survives a pod restart because it's persisted outside the pod.

| Store | What it holds | Why it's there |
|---|---|---|
| **Postgres** | Chargers, transactions, reservations, charging profiles, local auth lists, security events, charge-point credentials, certificate mirror, pending CSRs. | Relational state with a clear schema. Audit-grade. |
| **Redis** | Online registry (which pod has which `cp_id`), cross-pod command bus (`RemoteStart` to a charger on another pod), Authorize cache, idempotency cache for replays. | Sub-millisecond hot path; ephemeral by design. |
| **ClickHouse** | Time-series telemetry — `MeterValues` samples, status history, boot history, transaction starts. | Hundreds of thousands of inserts per minute at fleet scale; bad shape for Postgres. |
| **Kafka** | The event firehose itself. | Decouples producers (the gateway) from consumers (your platform). |

The gateway itself holds only one thing in memory: the live WebSocket per charger plus a small cache of metadata. Lose a pod, lose those sockets — chargers reconnect to another pod within seconds and the rest of the state is intact.

---

## Horizontal scaling in one paragraph

Chargers do not pick a pod. Envoy does — by computing a consistent hash of the `cp_id` and steering the WebSocket upgrade to the same pod each time. When your backend wants to send `RemoteStart` to charger `cp_id=X`, the gateway pod that receives the REST call doesn't necessarily own `X`'s socket. It looks up `X` in the Redis online registry, finds the pod ID that owns it, and forwards the command over a Redis pub/sub bus. The owning pod dispatches the OCPP CALL on the socket, gets the reply, hands it back. Your REST call sees a single response with the charger's verdict.

This means three things in practice: the gateway scales horizontally by adding pods; chargers reconnect to whichever pod they land on; and a single backend call always returns the right answer regardless of which pod served it.

[`concepts/multi-pod-and-routing.md`](./concepts/multi-pod-and-routing.md) goes into the mechanics.

---

## What the gateway is *not* responsible for

Worth restating with the diagram in mind:

- **User authentication** lives in your backend. The gateway forwards `Authorize` to your backend via a hot-path REST call you implement.
- **Billing** lives downstream of the events. The gateway publishes `tx.started` and `tx.stopped`; your billing service consumes them.
- **The smart-charging algorithm** runs on the charger itself. The gateway transports charging profiles; the charger resolves which is active.

The boundary is deliberate. The gateway is the *only* component that has to know OCPP intimately; everything else on your platform sees a clean, OCPP-free contract.

---

## Where to go from here

- **Want to operate this?** [`guides/install.md`](./guides/install.md) then [`guides/deploy-to-production.md`](./guides/deploy-to-production.md).
- **Want to integrate?** [`guides/use-the-rest-api.md`](./guides/use-the-rest-api.md) and [`guides/consume-events.md`](./guides/consume-events.md).
- **Want the deeper story of how a charging session flows?** [`concepts/how-ocpp-flows-work.md`](./concepts/how-ocpp-flows-work.md).
