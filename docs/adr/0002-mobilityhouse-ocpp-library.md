# ADR-0002 — Adopt `mobilityhouse/ocpp` as the protocol library

- **Status**: Accepted
- **Date**: 2026-04-29
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

The OCPP-J protocol is large and evolving:

- **OCPP 1.6** — ~25 actions, errata v4.
- **OCPP 2.0.1** — ~70 actions, Editions 2 and 3.
- **OCPP 2.1** — newest, bidirectional charging, ISO 15118-20 support.

Each action has a request schema, a response schema, and (in 2.x) Device Model variables. The Open Charge Alliance ships official JSON Schemas; getting parsing and validation wrong silently produces invalid messages on the wire.

Building this protocol layer ourselves is a 3–6 month detour and an ongoing maintenance burden as errata land. Hand-rolled OCPP stacks are a known source of subtle wire-format bugs.

Constraints:

- We chose Python ([ADR-0001](./0001-python-asyncio-stack.md)).
- We need 1.6 from day one and 2.0.1 within a year.
- We must be able to certify against OCTT.
- License must permit commercial use.

## Decision

**Adopt [`mobilityhouse/ocpp`](https://github.com/mobilityhouse/ocpp) (the official Python OCPP library, MIT-licensed) as the foundation of `eveys/ocpp`.**

We use it for:

- Wire-format parsing/serialization (`Call`, `CallResult`, `CallError`).
- JSON-Schema validation against OCA-shipped schemas.
- Typed dataclasses for every action (`call.BootNotification`, `call_result.MeterValues`, …).
- The `@on()` decorator for routing inbound actions to handlers.
- The `ocpp.v16`, `ocpp.v201`, `ocpp.v21` parallel subpackages — we use the same isolation in our own code.

Around this library, **we build**:

- WebSocket server (we use the `websockets` package, not bundled).
- Authentication (mTLS or Basic Auth at the edge).
- Persistence (Postgres + Redis + Kafka).
- gRPC API for the rest of the platform.
- Operational concerns (registry, command bus, idempotency, observability).

## Alternatives considered

- **Hand-roll the OCPP protocol layer in Python** — full control, full burden. Spec drift over time. Rejected: ~3 months of work that adds zero customer value.
- **Adopt CitrineOS (TypeScript)** — modern, modular, OCPP 2.0.1-first. Rejected because we chose Python (ADR-0001); not a fit for our team. Worth revisiting only if we pivot language.
- **Adopt SteVe (Java)** — mature, OCPP 1.6 only. Rejected: Java mismatch with team, single-version scope, and SteVe is a full CSMS, not a library.
- **Adopt `ocpp-go` (Go)** — community library, smaller maintainer base. Rejected: language mismatch (ADR-0001).
- **Roll our own minimal subset for 1.6 only** — tempting for time-to-MVP, but creates technical debt the moment we need 2.0.1, and we'd be writing JSON-Schema-validation by hand. Rejected.

## Consequences

### Positive

- Zero protocol code in `eveys/ocpp` — we focus on integration and operations.
- Multi-version support out of the box (1.6 / 2.0.1 / 2.1 all in one library).
- Production pedigree: `mobilityhouse/ocpp` is maintained by The Mobility House, an active commercial CSMS operator.
- MIT license — no copyleft risk.
- Schema-driven: validation catches charger firmware bugs at the boundary, not in business logic.
- We benefit from upstream errata fixes for free.

### Negative / costs

- **External dependency.** We depend on a single upstream maintainer. Mitigation: MIT license means we own the source if upstream stagnates; we can fork.
- **Library API changes** between major versions could force migrations. Mitigation: pin to a specific version; upgrade in scheduled cadence.
- **Learning curve** for engineers new to the library.

### Risks

- **Library bug surfaces in production** — same risk as any third-party dep. Mitigation: comprehensive integration tests; ability to monkey-patch or fork urgently.
- **Library doesn't keep up with OCPP 2.1 ratified errata** — Mitigation: community contributions; we can submit fixes upstream.
- **License change** — MIT can't be revoked retroactively, but a future major version could relicense. Mitigation: pin known-good versions; keep our usage at the public API only.

### Reversibility

- **Highly reversible.** The library boundary is narrow (parser + dataclasses + decorator). If we ever need to swap, we replace ~5 internal modules. The handler bodies, persistence, gRPC, and operations are all our code and don't change.

## Project conventions implied by this decision

- Strict version isolation: `ocpp.v16`, `ocpp.v201`, `ocpp.v21` never cross-import. Our code mirrors this in `src/eveys_ocpp/handlers/v16/`, `v201/`, etc.
- JSON Schemas in `ocpp/<version>/schemas/` are authoritative. Our dataclasses don't override or "loosen" them.
- We never disable schema validation, even under load.

These conventions are restated in [`03-coding-standards.md`](../03-coding-standards.md).

## References

- [`mobilityhouse/ocpp` GitHub](https://github.com/mobilityhouse/ocpp)
- [PyPI](https://pypi.org/project/ocpp/)
- [OCA — OCPP specification downloads](https://www.openchargealliance.org/protocols/ocpp/)
