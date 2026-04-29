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

## Pending / planned

ADRs to be written as decisions become active:

| Topic | Why |
|---|---|
| 0006 | Postgres + Redis + Kafka data plane (vs alternatives) |
| 0007 | Envoy as the LB (vs Nginx / NLB) |
| 0008 | gRPC for internal API (vs REST / GraphQL) |
| 0009 | OCPP version isolation rule (no cross-import between v16/v201/v21) |
| 0010 | Idempotency model |
| 0011 | Authentication strategy (Basic Auth → mTLS path) |
| 0012 | Rollout strategy: gated waves with per-`cp_id` allowlist |
| 0013 | OCPP audit-log retention and archival |
| 0014 | Schema Registry choice for Kafka events |

## How to contribute an ADR

1. Copy `adr/template.md` to `adr/NNNN-short-title.md` (use the next available number).
2. Set status to `Proposed`.
3. Open an MR. Tag relevant reviewers.
4. After approval, change status to `Accepted` and merge.
5. Update the index above.

Discussion happens **on the MR**, not in chat. The ADR plus its review thread are the historical record.
