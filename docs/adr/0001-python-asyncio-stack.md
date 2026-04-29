# ADR-0001 — Python 3.13 + asyncio as the primary runtime

- **Status**: Accepted
- **Date**: 2026-04-29
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

`eveys/ocpp` is a green-field service. The defining workload characteristic is **long-lived stateful WebSocket connections** (one per charger, 24/7). Each pod will hold thousands of sockets simultaneously and process bursts of small messages with strict ordering per charger.

This workload doesn't fit a per-request runtime model: any synchronous in-process work would block the OCPP message loop, and process restarts must not cascade into fleet-wide reconnect storms. We need a runtime built for many-concurrent-long-lived connections.

Constraints:

- Existing engineering team is Python-fluent.
- The protocol library we want to build on (`mobilityhouse/ocpp`) is Python-only.
- Time-to-production: ~14 weeks AI-accelerated.
- Must support OCPP 1.6, 2.0.1, and 2.1 (now or near future).
- Per-pod target: 5–10k concurrent connections.

## Decision

**Python 3.13 + `asyncio` + `uvloop`** is the runtime for `eveys/ocpp`.

- Standard library: `asyncio` (no Trio, no Twisted).
- Production event loop: `uvloop` (drop-in replacement for the default `asyncio` loop, 2–4× faster).
- Web/WS server: `websockets` ≥ 13.
- Internal RPC: gRPC via `grpclib` (async-native; not the C-based `grpcio` which has its own thread pool).
- DB driver: `asyncpg` directly for hot paths, SQLAlchemy 2.0 async for admin/CRUD.

## Alternatives considered

- **Go** (`ocpp-go`) — ~3× connection capacity per process, but the OCPP library is community-maintained, less battle-tested than `mobilityhouse/ocpp`. Rewriting protocol code adds 3–6 months. Team is not Go-fluent. Rejected: cost outweighs benefit at our scale.
- **Elixir / Erlang (BEAM)** — architecturally beautiful for one-process-per-charger, but the OCPP ecosystem is tiny (`jarl_ocpp`), hiring is hard, and we'd be the largest user of any library we picked. Rejected: ecosystem risk.
- **Node.js / TypeScript with CitrineOS** — viable; if we already had a strong TS team, we'd seriously consider adopting CitrineOS instead of building. Rejected: language mismatch with our team.
- **Rust** — best raw performance, but no production OCPP library exists. We'd write protocol + business logic + ops tooling all from scratch. Rejected: unjustified risk and timeline.

## Consequences

### Positive

- Zero protocol code: `mobilityhouse/ocpp` covers 1.6, 2.0.1, 2.1.
- Team velocity: existing Python skills transfer directly.
- AI-assisted delivery is most effective in Python (largest training corpora).
- Linear horizontal scale-out: each pod holds 5–10k connections; we add pods to grow.

### Negative / costs

- ~3× more pods than a Go equivalent for the same fleet size. Acceptable up to ~320k chargers.
- `asyncio` debugging is harder than synchronous code; team needs async fluency.
- One process per pod (no multi-worker) means we can't trivially exploit multiple cores in a single pod. Mitigated by sizing pods at 2 vCPU and scaling horizontally.

### Risks

- **GIL contention** if we accidentally do CPU-heavy work on the loop. Mitigation: profile quarterly; move heavy work to `loop.run_in_executor` thread pools.
- **`uvloop` quirks** with edge libraries. Mitigation: stay on stable `uvloop` releases; have fallback to default loop ready.
- **`grpclib` is less mature than `grpcio`.** Mitigation: contracts are simple; we're not relying on advanced features.

### Reversibility

- Switching language later is a **one-way door** in practice (full rewrite). Decision must hold for the lifetime of the service.
- Switching libraries within Python (e.g. `aiohttp` → `starlette`) is reversible at integration-test cost.

## References

- [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) — the library this decision implies.
- Python 3.13 release notes: <https://docs.python.org/3/whatsnew/3.13.html>
- `asyncio` docs: <https://docs.python.org/3/library/asyncio.html>
- `uvloop`: <https://github.com/MagicStack/uvloop>
