# What is eveys/ocpp?

**Audience.** Anyone evaluating this project for the first time.

**What this answers.** What the gateway does, what OCPP is in 60 seconds, where this fits in a charging platform, and what it deliberately does not do.

---

## In one paragraph

**eveys/ocpp** is a production-ready gateway service for an EV-charging platform. It owns every EV charger's WebSocket connection, speaks the OCPP protocol on that socket, and gives the rest of your platform a clean, stable surface — REST and gRPC for sending commands, Kafka and webhooks for receiving events. It is horizontally scalable, fully containerised, and Apache-2.0 licensed.

---

## OCPP in 60 seconds

OCPP — the **Open Charge Point Protocol** — is the industry-standard language EV chargers use to talk to a central management system. A charger opens a single long-lived **WebSocket** connection to that system on boot and keeps it open for its entire operational life, reconnecting if the link drops.

Over that socket, two flows happen:

1. **The charger initiates.** "I just booted." "Here's a heartbeat." "User RFID-12345 tapped, can they charge?" "Here's a meter reading every 30 seconds." "The session just ended; the meter went from 12,345 Wh to 18,902 Wh."
2. **The central system initiates.** "Start charging on connector 1 for user RFID-12345." "Stop the current session." "Reset yourself." "Unlock the cable."

Both directions use the same JSON-RPC-ish envelope; each message has a schema and a tightly bounded set of allowed values. The protocol is the contract between the charger manufacturer and the platform.

The two versions that matter today are **OCPP 1.6-J** (the de-facto incumbent, widely deployed) and **OCPP 2.0.1** (the security-hardened successor, gaining ground). This gateway speaks both.

You do not need to be an OCPP expert to use this project. The terms you will meet — `BootNotification`, `Authorize`, `StartTransaction`, `MeterValues`, `StopTransaction`, `RemoteStart` — each get a one-line definition in [`04-glossary.md`](./04-glossary.md), and the end-to-end picture is in [`concepts/how-ocpp-flows-work.md`](./concepts/how-ocpp-flows-work.md).

---

## What this gateway owns

It owns the WebSocket. That's the load-bearing point.

Every charger in your fleet has exactly one TCP/TLS WebSocket connection open to this gateway. Some of those chargers are sending a heartbeat every 30 seconds; some are mid-session reporting meter readings every 60 seconds; some are idle but online; some are in a fault state.

The gateway:

- **Terminates the WebSocket.** Negotiates the OCPP subprotocol (`ocpp1.6` or `ocpp2.0.1`), authenticates the charger (Basic Auth, mTLS at the edge, or both), and keeps the socket alive.
- **Schema-validates every message** against the published OCPP JSON schemas. Malformed messages are rejected at the wire boundary; your platform never sees them.
- **Routes inbound events.** Persists what needs persisting (transactions, status, telemetry), and publishes envelopes to Kafka topics for downstream consumers — your billing engine, your dashboards, your alerting.
- **Dispatches outbound commands.** When your backend says "remote start", the gateway finds the right charger's socket (even if it's on a different pod), serialises the OCPP `RemoteStartTransaction.req`, sends it, waits for the reply, and returns the result.
- **Survives reconnects, restarts, and pod failures.** Chargers reconnect to whichever pod they land on; the gateway tracks "which pod has which charger" through a Redis-backed online registry and routes commands across pods over a pub/sub bus.

---

## What this gateway does *not* own

A common source of confusion: the gateway is **not** the rest of your platform. It does not:

- **Authenticate end users.** When a user taps an RFID card, the gateway receives an `Authorize` request from the charger and asks *your backend* "is this user allowed to charge?". Your backend owns the user database, the wallet, the tariff. The gateway forwards the answer to the charger verbatim.
- **Bill or invoice.** The gateway publishes session start, session stop, and meter readings. Your billing service consumes those events and produces invoices.
- **Manage chargers as physical assets.** Site IDs, asset tags, geo-location, install date — those belong in your asset-management system. The gateway only cares about each charger's logical identifier (`cp_id`) and current connectivity state.
- **Run the smart-charging algorithm.** It accepts charging profiles from your platform and forwards them to chargers; the chargers themselves resolve which profile is active at any moment. The gateway is the transport, not the optimiser.
- **Provide a user-facing UI.** It exposes REST and gRPC for your apps and dashboards to consume. Building those is your team's job.

The boundary is deliberate. The gateway is the single component that has to know OCPP intimately; everything else on your platform consumes a clean, OCPP-free contract.

---

## How it fits into a charging platform

```
                                Chargers
                                   │ WSS / OCPP-J
                                   ▼
                         ┌──────────────────┐
                         │   eveys/ocpp     │   ← you are here
                         │   the gateway    │
                         └────┬──────┬──────┘
                              │      │
        REST + gRPC (commands)│      │ Kafka + webhooks (events)
                              │      ▼
                              │   ┌────────────────────────────────┐
                              │   │ Your platform                  │
                              └──►│  (auth · sessions · billing ·  │
                                  │   dashboards · alerting · ...)  │
                                  └────────────────────────────────┘
```

Your platform reaches chargers *through* the gateway. It learns about charger events by tailing Kafka topics or by receiving webhook deliveries. It never opens a WebSocket of its own.

That asymmetry is the whole product:

- **The gateway speaks OCPP** so you don't have to write a single line of `mobilityhouse/ocpp` glue in your business code.
- **Your platform speaks REST, gRPC, Kafka, and webhooks** — surfaces every backend team already knows.

---

## What you get out of the box

- **OCPP 1.6 Core profile**: BootNotification, Heartbeat, StatusNotification, Authorize, StartTransaction, MeterValues, StopTransaction, DataTransfer — every inbound message handled and persisted; every outbound command (RemoteStart, RemoteStop, Reset, UnlockConnector, ChangeAvailability, ChangeConfiguration, GetConfiguration, ClearCache, TriggerMessage, DataTransfer) dispatched and round-tripped.
- **OCPP 1.6 LocalAuthList**, **Reservations**, **SmartCharging**, **Firmware Management**, and the **Security Whitepaper** extensions (security events, signed firmware updates, certificate install/delete, log retrieval, CSR signing). Coverage continues to expand toward OCPP 2.0.1.
- **REST API** for backend integration, with bearer-token auth, cursor pagination, a stable error envelope, and an OpenAPI schema served at `/api/v1/openapi.json`.
- **gRPC API** for the same operations at lower overhead; protobuf definitions are part of the release.
- **Kafka event topics** carrying a versioned protobuf envelope: `cp.boot`, `cp.status`, `cp.meter`, `cp.connected`, `cp.disconnected`, `cp.firmware_status`, `cp.diagnostics_status`, `cp.security_event`, `cp.csr_submitted`, `tx.started`, `tx.stopped`.
- **Webhooks** carrying the same envelopes for backends that prefer push delivery to consuming Kafka.
- **Prometheus metrics**, structured JSON logs, OpenTelemetry traces.
- **Horizontal scaling.** Envoy ring-hashes chargers to pods by `cp_id`; the gateway uses Redis as an online registry and pub/sub bus so commands routed to the wrong pod find their way home. Pods drain cleanly on `SIGTERM`.
- **Helm chart and Docker images** ready for production use.

---

## What it costs to run

The runtime stack is:

- **The gateway pods** themselves — Python 3.13 on `asyncio` + `uvloop`. Each pod handles hundreds to low-thousands of concurrent charger sockets depending on traffic pattern and instance size.
- **Postgres** for relational state (chargers, transactions, reservations, charging profiles, security events).
- **Redis** for the online registry, the cross-pod command bus, the Authorize cache, and idempotency tracking.
- **Kafka** for the event firehose.
- **ClickHouse** for time-series telemetry (meter readings, status history). Optional but recommended at scale.
- **Envoy** in front of the pods, providing TLS termination and ring-hash routing on `cp_id`.

The Quickstart ([`02-quickstart.md`](./02-quickstart.md)) brings all of these up locally via Docker Compose in about 30 seconds.

---

## What this gateway is not

- **Not a charger emulator.** It speaks OCPP *to* chargers; it doesn't pretend to *be* one.
- **Not a billing system.** It publishes events; the rest is yours.
- **Not OCPP-certified yet.** Implementation is complete and unit-tested against the published schemas; formal OCTT (Open Charge Alliance Testing Tool) certification requires OCA membership and is on a separate track. Until that lands, treat the gateway as conformant-by-implementation rather than conformant-by-certification.
- **Not bundled with a frontend.** This is a gateway service, not a platform-in-a-box.

---

## Where to go from here

- Trying it for the first time: [`02-quickstart.md`](./02-quickstart.md).
- Need to understand the surfaces before integrating: [`03-architecture.md`](./03-architecture.md).
- Coming back to look up an OCPP term: [`04-glossary.md`](./04-glossary.md).
