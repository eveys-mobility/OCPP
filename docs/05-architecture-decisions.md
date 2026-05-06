# 05 — Architecture Decision Records

> ADRs capture the **why** of significant decisions, so future engineers (and future-us) can understand the constraints we lived under.

## Why ADRs?

Code shows *what*. Commit messages show *what changed*. Neither shows *why we chose this over the alternatives*. Without that, every refactor is a coin flip, and every onboarding takes weeks.

ADRs are short (1–2 pages), append-only, and decision-scoped. **One decision per ADR.**

## When to write one

Write an ADR when you:

- Pick a technology that's load-bearing (DB, language, queue, RPC, observability).
- Decide *not* to do something obvious (and want to remember why a year from now).
- Establish a project-wide convention (naming, layout, error model).
- Diverge from a coding standard documented in `03-coding-standards.md`.
- Cross a one-way door (security model, API surface, persistence model).

Don't write an ADR for:

- Implementation details that can be refactored without touching contracts.
- Style preferences (those go in `03-coding-standards.md`).
- Bug fixes.

## Format

Every ADR uses this template (in `adr/template.md`):

```markdown
# ADR-NNNN — Short title

- **Status**: Proposed | Accepted | Deprecated | Superseded by ADR-MMMM
- **Date**: YYYY-MM-DD
- **Author**: Name
- **Reviewers**: Names

## Context

What's the problem? What constraints exist?

## Decision

What did we choose? State it clearly in 1–3 sentences.

## Alternatives considered

What did we evaluate and reject? Why?

## Consequences

What does this enable? What does it cost? What new problems does it create?

## References

Links to relevant docs, RFCs, prior art.
```

## Lifecycle

- **Proposed** → under discussion. Open for comment.
- **Accepted** → approved by tech lead + at least one reviewer. Merge to `main`.
- **Deprecated** → no longer applies, but historical context is useful.
- **Superseded** → a newer ADR replaces it. Both link to each other.

ADRs are **never deleted**. They are append-only history.

## Index

| # | Title | Status | Date |
|---|---|---|---|
| [0001](./adr/0001-python-asyncio-stack.md) | Python 3.13 + asyncio as the primary runtime | Accepted | 2026-04-29 |
| [0002](./adr/0002-mobilityhouse-ocpp-library.md) | Adopt `mobilityhouse/ocpp` as the protocol library | Accepted | 2026-04-29 |
| [0003](./adr/0003-monorepo-layout.md) | Monorepo layout (`eveys/<service>`) | Accepted | 2026-04-29 |
| [0004](./adr/0004-clickhouse-timeseries-store.md) | ClickHouse as the time-series store | Accepted | 2026-04-29 |
| [0005](./adr/0005-certification-target.md) | Certification target: OCPP 1.6 CSMS, all profiles | Accepted | 2026-04-29 |
| [0015](./adr/0015-kafka-event-envelope-format.md) | Kafka event envelope format (one envelope, five topics, `cp_id` partition key) | Accepted | 2026-04-30 |
| [0016](./adr/0016-cross-pod-command-bus.md) | Cross-pod command bus over Redis pub/sub | Accepted | 2026-04-30 |
| [0017](./adr/0017-idempotency-cache.md) | Idempotency cache for inbound OCPP replays | Accepted | 2026-04-30 |
| [0018](./adr/0018-grpc-backward-compat-enforcement.md) | gRPC + Kafka-event backward-compat enforced in CI | Accepted | 2026-05-01 |
| [0019](./adr/0019-kafka-producer-hardening.md) | Kafka producer hardening: durability over throughput | Accepted | 2026-05-01 |
| [0020](./adr/0020-clickhouse-ingestion-sidecar.md) | ClickHouse ingestion: sidecar over Kafka Engine | Accepted | 2026-05-01 |
| [0021](./adr/0021-reservations-charger-authority.md) | Reservations: charger-side authority + gateway-side mirror | Accepted | 2026-05-05 |
| [0022](./adr/0022-smart-charging-charger-side-resolver.md) | Smart Charging: charger-side resolver, gateway-side profile mirror | Accepted | 2026-05-05 |
| [0023](./adr/0023-backend-rest-integration.md) | Backend REST integration: two surfaces, asymmetric envelope | Accepted | 2026-05-05 |
| [0027](./adr/0027-webhook-delivery.md) | Outbound webhook delivery: Kafka-tail, HMAC, exponential retry | Accepted | 2026-05-07 |

## Pending / planned

ADRs to be written as decisions become active:

| Topic | Why | Status |
|---|---|---|
| 0006 | Postgres + Redis + Kafka data plane (vs alternatives) | reserved |
| 0007 | Envoy as the LB (vs Nginx / NLB) | reserved |
| 0008 | gRPC for internal API (vs REST / GraphQL) | reserved |
| 0009 | OCPP version isolation rule (no cross-import between v16/v201/v21) | reserved |
| 0010 | Idempotency model | written as [ADR-0017](./adr/0017-idempotency-cache.md) |
| 0011 | Authentication strategy (Basic Auth → mTLS path) | reserved |
| 0012 | Rollout strategy: gated waves with per-`cp_id` allowlist | reserved |
| 0013 | OCPP audit-log retention and archival | reserved |
| 0014 | Schema Registry choice for Kafka events | reserved |

Numbers are reserved up front so newcomers don't accidentally collide. When a reserved number's topic is picked up, write the ADR under that number rather than allocating a fresh one. (`0010` is the exception above — it was deferred until after E2-11 design crystallized, by which point the next-available slot was `0017`.)

## How to contribute an ADR

1. Copy `adr/template.md` to `adr/NNNN-short-title.md` (use the next available number).
2. Set status to `Proposed`.
3. Open an MR. Tag relevant reviewers.
4. After approval, change status to `Accepted` and merge.
5. Update the index above.

Discussion happens **on the MR**, not in chat. The ADR plus its review thread are the historical record.
