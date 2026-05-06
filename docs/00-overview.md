# 00 — Overview

> **Eveys** is an EV-charging platform.
>
> **`eveys/ocpp`** is its dedicated **OCPP gateway service** — the single component that owns every charger's WebSocket connection and exposes a clean internal API for the rest of the platform.

## What `eveys/ocpp` is

A standalone, horizontally scalable Python service that:

- Speaks **OCPP-J 1.6** (and **2.0.1** as a parallel track) over WebSocket with EV chargers.
- Validates every message against the official OCPP JSON Schemas.
- Routes commands to chargers and publishes events from chargers.
- Exposes the rest of the Eveys platform a stable **gRPC API** (commands) and **Kafka topics** (events).
- Holds all charger sockets behind a sticky load balancer (Envoy, consistent-hash on `cp_id`).

Built on:

- **Python 3.13 + asyncio + uvloop**
- **[`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp)** — the official Python OCPP library
- **`websockets`** for transport
- **gRPC** (`grpclib` async) for internal API
- **Postgres** (state), **Redis** (registry / cache / pub-sub), **Kafka** (event firehose)
- **ClickHouse** (time-series store for `MeterValues`, `StatusNotifications`, `BootNotifications`, `StartTransactions`; Heartbeats are absorbed by the Redis online registry per ADR-0020 rather than persisted as time-series rows)
- **Kubernetes** for orchestration

## Where it fits in the Eveys platform

```
                                 Chargers
                                    │ WSS / OCPP-J
                                    ▼
                          ┌──────────────────┐
                          │  eveys/ocpp      │   ← this project
                          │  (the gateway)   │
                          └────┬───────┬─────┘
                               │       │
              REST + webhooks  │       │ Kafka events (firehose)
                               │       ▼
                               │   ┌──────────────────────────────┐
                               │   │ Eveys backend                │
                               └──►│  (auth · sessions · billing) │
                                   └──────────────────────────────┘
```

`eveys/ocpp` is **the only service that holds charger sockets**. The Eveys backend reaches chargers through `eveys/ocpp`'s REST API (or gRPC for the lower-overhead path); it learns about charger events by webhook delivery and / or by subscribing to Kafka topics.

## What `eveys/ocpp` does **not** own

- ❌ Drivers, accounts, RFID tokens — the backend owns user / authorization state.
- ❌ Billing, session-cost calculation — the backend owns the billing record; `eveys/ocpp` reports start/stop meter readings.
- ❌ Operator UI — separate concern; the backend or a downstream UI consumes the gateway's REST surface.
- ❌ ISO 15118, OCPI roaming — future, separate concerns.

When `eveys/ocpp` needs a decision (e.g. "is `id_tag=ABC123` authorized?"), it **calls out** to the backend over REST per the contract in [`docs/integration/`](./integration/README.md). It never imports the backend's database.

## What this project must do

OCPP at fleet scale has workload characteristics that justify a dedicated service:

- **Long-lived stateful WebSocket connections** — one per charger, 24/7. Each pod holds thousands of sockets simultaneously.
- **Strict per-charger message ordering** — out-of-order delivery silently breaks transaction state.
- **Bursty reconnect traffic** — pod restarts must not cascade into fleet-wide reconnect storms.
- **Time-series telemetry firehose** — `MeterValues` / `StatusNotifications` / `BootNotifications` / `StartTransactions` belong in Kafka and ClickHouse, not a relational store. (Heartbeats are presence pings handled in-memory by the Redis online registry — no time-series row written; see ADR-0020.)
- **Observable at per-charger granularity** — any incident must be localizable to a single `cp_id` in seconds.

These constraints don't fit a typical request/response service. `eveys/ocpp` is built specifically for them.

## Goals

1. **Dedicated runtime for long-lived WS connections.** No shared loop with HTTP/cron workloads.
2. **Survive rolling restarts.** No fleet-wide reconnect storms.
3. **Linear scale-out** with charger count. From day-one fleet to 320k chargers on the same architecture.
4. **Stable internal contract** (gRPC + Kafka). The rest of the platform doesn't touch sockets.
5. **Observable** at per-charger granularity. Localize any incident in < 5 minutes.
6. **OCTT-certified** for OCPP 2.0.1 within 12 months.

## Non-goals (explicit)

- Replacing the rest of the Eveys platform.
- Writing new payment / auth / session logic.
- Building consumer-facing UIs (mobile, web, admin) — those live in the Eveys backend / its downstream apps.
- Migrating chargers to OCPP 2.0.1 firmware (separate program).
- Building OCPI roaming or ISO 15118 plug-and-charge (future projects).

## Target deliverables

| Deliverable | Phase |
|---|---|
| Foundations docs (this set) | **Now** |
| Architecture decisions (ADRs) | **Now** |
| Implementation plan | After docs are approved |
| `ocpp-gw` MVP (5 core handlers, single pod) | Phase 1 |
| Full OCPP 1.6 Core, gRPC, Kafka | Phase 2 |
| Production rollout (10 → all chargers) | Phase 3 |
| OCPP 2.0.1 + OCTT certification | Phase 4 (parallel from week 4) |

See [01-roadmap.md](./01-roadmap.md) for dates, owners, and exit criteria.
