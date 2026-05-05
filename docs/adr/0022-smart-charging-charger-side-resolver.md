# ADR-0022 — Smart Charging: charger-side resolver, gateway-side profile mirror

- **Status**: Accepted
- **Date**: 2026-05-05
- **Author**: Eveys engineering (E2-1E; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

The OCPP 1.6 Smart Charging profile (E2-1E) is the protocol's mechanism for the CSMS to influence how a charger draws power: per-connector limits, time-windowed schedules, recurring patterns (Daily/Weekly), and stacked profiles with priority order (`ChargePointMaxProfile` > `TxDefaultProfile` > `TxProfile`). Three CSMS-initiated RPCs:

- `SetChargingProfile` — push a profile to the charger.
- `ClearChargingProfile` — remove profiles matching a filter set.
- `GetCompositeSchedule` — ask "what's the effective schedule for the next N seconds, accounting for stacking, expiry, and recurring unroll?"

The `GetCompositeSchedule` answer is non-trivial. The OCPP spec describes the **resolver algorithm** in section 3.13.4 — at a given point in time, walk the active profiles in priority order, intersect their time windows, unroll any `Recurring` profile to its next firing, take the lowest applicable limit per period, and produce a flat list of `(start_offset, limit)` periods covering the requested duration.

Two questions need a clear answer:

1. **Where does the resolver live?** Charger-side, gateway-side, or both?
2. **What does the gateway-side `charging_profiles` table store, and what does it not?**

Forces:

- The charger already runs the resolver — it's the OCPP-mandated source of truth for power delivery in real time. Reproducing that algorithm on the gateway side means two implementations to keep in sync; any divergence is a real bug operators have to debug.
- Operator dashboards / mobile BFF want to render "what limit is active right now" without an OCPP round-trip per page-load. A gateway-side `charging_profiles` table makes per-charger profile listing O(1).
- Charger clocks drift. Recurring schedules are anchored on the charger's local time. Gateway-side resolution would have to use the charger's clock (not its own) to match the charger's view — and we have no reliable way to query that clock for a profile that fires in 4 hours.
- The same "charger authority + gateway mirror" pattern was set in ADR-0021 (Reservations) and ADR-0017 (idempotency cache). Picking a third pattern just for Smart Charging fragments the project's mental model.

## Decision

**The charger is the source of truth for the composite schedule; the gateway stores the *input* profiles in a `charging_profiles` (+ `charging_schedule_periods`) table mirror. `GetCompositeSchedule` is a charger round-trip — the gateway does not implement the OCPP § 3.13.4 resolver.**

Concretely:

1. **`SetChargingProfile`** — charger first. On charger `Accepted`, upsert the profile in the gateway mirror, keyed on `(charge_point_id, charging_profile_id)`. The OCPP-wire `chargingProfileId` is operator-supplied; "replace this profile" maps cleanly to ON CONFLICT DO UPDATE on that natural key. Schedule periods are wiped + reinserted (a profile change replaces its schedule wholesale per spec).

2. **`ClearChargingProfile`** — charger first. On `Accepted`, mark matching rows as `Cleared` in the gateway mirror. Don't delete — the row stays for audit and analytics. (A Phase-5 cleanup task can prune Cleared rows older than N days.)

3. **`GetCompositeSchedule`** — charger round-trip. Gateway forwards (`connector_id`, `duration`, optional `charging_rate_unit`) verbatim and translates the charger's reply (`status`, `connector_id`, `schedule_start`, list of `ChargingSchedulePeriod`) to the matching proto messages. The gateway does not reach into its own `charging_profiles` table to compute a parallel answer; the charger's reply is what we ship.

4. **What the mirror is for.** Operator dashboards listing "all profiles on charger X". Analytics on "which profiles overlapped on which day". Differential maintenance ("the charger has profile 42, do we still want it?"). It is **not** a real-time resolver and never returns a "current limit" without a charger round-trip.

5. **No new ADR each time we add a profile-shaped table.** ADR-0021 already set the pattern (Reservations). This ADR notes that Smart Charging is the third application of the same shape and explicitly defers resolver reimplementation as out of scope for v1.

## Alternatives considered

- **CSMS-side resolver (full implementation of OCPP § 3.13.4).** Reject. The algorithm is well-specified but non-trivial: profile stacking with priority, recurring-schedule unroll across the requested window, period-level limit minimisation, valid-from/valid-to clipping, and Absolute-vs-Relative time-anchor normalisation. Reproducing it would take ~3-5 days of careful work and a comparison test suite vs the charger's output. The maintenance cost is open-ended: every OCPP errata that touches the resolver becomes our problem to track. **No operator workflow today needs gateway-side resolution.** When one does (Phase 4 dashboards likely), we revisit — the data is there in the mirror; the resolver is just code.

- **Don't store profiles at all; round-trip every operator query.** Reject. A "list all profiles on charger X" operator query becomes a `GetCompositeSchedule(duration=86400)` per connector, which is overkill: the operator wants the *configuration*, not the resolved schedule. The mirror gives them a typed, queryable, fast answer.

- **Single-table layout with JSONB for periods.** Reject for the same reason ADR-0021 picked two tables for LocalAuthListEntry: a profile update wholesale-replaces its schedule, but per-period analytics queries (Phase 4) are easier when periods live in their own table. The cost is one extra `INSERT` per period; profile updates are infrequent.

## Consequences

### Positive

- Mental-model continuity with ADR-0017 / ADR-0021: the project has one consistent "charger authority + gateway mirror" pattern across Reservations, Smart Charging, and idempotency caching.
- Fast operator-side profile listing (Postgres SELECT, not OCPP round-trip).
- No resolver bug surface to debug: the answer is whatever the charger returns.

### Negative / costs

- **No real-time gateway-side limit query.** Apps that want "current power cap on charger X" must use `GetCompositeSchedule` (a charger round-trip). For an operator dashboard polling every 30s across N chargers, that's N OCPP calls per cycle. Acceptable today (we have ~10 dev chargers); will hurt at fleet scale and is the obvious Phase-4 trigger to revisit.
- **Stacked-profile semantics live only in the charger.** A `TxProfile` that overrides a `TxDefaultProfile` is invisible to analytics queries that just look at `charging_profiles` rows — both look "Active". Analytics has to encode the priority/window arithmetic if they want a true effective view. Documented in the table comment.
- **Cleared rows accumulate.** A long-lived charger that cycles through dozens of profiles per day grows `charging_profiles` linearly. Phase 5 cleanup task: prune `status='Cleared'` rows older than 90 days (or move to ClickHouse for analytics retention).

### Risks

- **Operator pushes a profile, charger Accepts, gateway mirror write fails.** Same risk class as Reservations; same mitigation (log, don't promote to gRPC error — the charger genuinely has the profile; a follow-up `GetCompositeSchedule` would surface the divergence). The MR commits this rationale in the SetChargingProfile body comment.
- **Operator pushes a profile, gateway upsert succeeds, charger reboots before honouring it.** Charger discards local profile state on reboot. Mirror still says `Active`. Same drift risk as Reservations; same mitigation (operator-driven reconciliation, not automatic).

### Reversibility

Reversible. Implementing the gateway-side resolver later is purely additive: the input data lives in the mirror already; we add a new module `charging/resolver.py` and a new gRPC RPC `GetCompositeScheduleLocal` (separate name to keep the charger-authoritative `GetCompositeSchedule` intact). Estimated migration: 3-5 days of resolver implementation + a comparison test suite running both gateway-side and charger-side answers on a real OCPP simulator.

## References

- [ADR-0017](./0017-idempotency-cache.md) — original "external truth, gateway is best-effort cache" framing.
- [ADR-0021](./0021-reservations-charger-authority.md) — same pattern for Reservations; this ADR is the third application.
- [`docs/02-tasks.md`](../02-tasks.md) — E2-1E row.
- OCPP 1.6 Edition 2 §6.46 (`SetChargingProfile.req`), §6.47 (`SetChargingProfile.conf`), §6.4 (`ClearChargingProfile.req`), §6.5 (`ClearChargingProfile.conf`), §6.9 (`GetCompositeSchedule.req`), §6.10 (`GetCompositeSchedule.conf`), §3.13 ("Smart Charging" general) — especially §3.13.4 ("Composite schedule").
