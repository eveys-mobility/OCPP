# ADR-0021 — Reservations: charger-side authority + gateway-side mirror

- **Status**: Accepted
- **Date**: 2026-05-05
- **Author**: Eveys engineering (E2-1C; AI-assisted draft, human-reviewed and merged)
- **Reviewers**: Project tech lead (post-merge sign-off)

## Context

The OCPP 1.6 Reservations profile (E2-1C) introduces two CSMS-initiated RPCs — `ReserveNow` and `CancelReservation` — plus an implicit lifecycle: a reservation is *consumed* when the charger sees a `StartTransaction` from the matching `id_tag` / `parent_id_tag` on the reserved connector before `expiry_date`, or *expired* on the charger's clock when the deadline passes.

Two design questions need a clear answer for `eveys/ocpp` and they're load-bearing for every later operator workflow that touches reservations:

1. **Who's the source of truth for "is this connector reserved right now"?** The charger or the gateway?
2. **Who assigns `reservation_id`?** OCPP doesn't say "it must come from the CSMS" but the field is described as "an identifier provided by Central System" — so the CSMS does. We need to decide where on the CSMS side.

The two places that matter for #1:

- The charger's local state machine — what it actually enforces when a `StartTransaction` comes in for a connector with a live reservation.
- A potential gateway-side `reservations` table in Postgres that operators / mobile-BFF / analytics could query directly without round-tripping the WebSocket.

Forces:

- Operator dashboards and the mobile BFF want to render "available connectors at this site" without N round-trips. A gateway-side mirror solves that.
- Reservations expire on a wall clock. Two clocks (charger + gateway) drift; if both enforce, the order of "expired vs. consumed" gets ambiguous.
- A reservation that's `Accepted` by the charger but missing from the gateway DB is a real possibility (e.g. the gateway crashed between charger reply and DB write). Same in reverse — gateway has a row, charger doesn't (e.g. charger reboot wiped its local state).
- The same problem already showed up for LocalAuthList (E2-1B) and we picked **charger authority + gateway mirror, persist only on Accepted** there. Picking the same shape here keeps the project's reservation-of-state mental model consistent.

## Decision

**Charger is the source of truth for reservation state. The gateway mirrors what the charger Accepted in a `reservations` table; on any non-Accepted reply the gateway does not write.**

Concretely:

1. The gateway assigns `reservation_id` as a BigInt at the gateway boundary. Implementation: `INSERT ... RETURNING id` against `reservations` with `status = 'Pending'`, then forward to the charger with the freshly-assigned ID.
2. On charger `Accepted`: flip the row's `status` to `Active`, persist the rest of the metadata (`expiry_date`, `id_tag`, `parent_id_tag`, `connector_id`).
3. On charger `Occupied` / `Faulted` / `Rejected` / `Unavailable`: delete the pending row (it never came alive). Do not leave it as a tombstone — the charger never accepted it; the operator gets the proto status back and can retry.
4. `CancelReservation`: forward the charger's `reservation_id`. On `Accepted` flip the row to `status = 'Cancelled'`. On `Rejected` leave it (the reservation either never existed or was already consumed; the charger's view wins).
5. **No gateway-side enforcement of expiry.** A row whose `expiry_date < now()` is *implicitly* expired; queries compute the effective status. No scheduler. (Phase 5 may add a periodic cleanup job; not needed today.)
6. **No gateway-side enforcement of consumption.** When the charger sees a matching `StartTransaction`, it stops honouring the reservation. The gateway learns this when the next operator query reads the live status from the charger — but the row stays as "active until expiry" in the mirror. Mismatch is acceptable for v1; Phase 4 / 5 may add a `transaction_id` column linking the reservation to the consuming transaction once we're ready to hook the StartTransaction handler.

The shape mirrors ADR-0020 (ClickHouse — sidecar over Kafka Engine) and the LocalAuthList pattern in E2-1B: the gateway is a mirror, the device is the source of truth. Engineers who learned that pattern for one event-table or list will recognise it instantly.

## Alternatives considered

- **CSMS-side authority** — gateway pre-validates "is this connector free" before forwarding. The charger's reply confirms. **Rejected**: the charger already does this validation (it has the live connector status, an expiry timer running locally, and authoritative knowledge of its own faulted/unavailable state). Pre-validation in the gateway just adds a stale view that has to be reconciled on every operator action. The added latency (a Postgres read before every `ReserveNow`) buys nothing because the charger reply is the deciding answer anyway.

- **Operator-supplied `reservation_id`** — let the gRPC caller specify the integer. **Rejected**: every operator service would need its own ID-allocator and we'd have to police global uniqueness. Gateway-assigned IDs (sequential BigInt, same as `transaction_id`) means no coordination needed and `(reservation_id, charge_point_id)` is unique by construction.

- **No gateway-side mirror at all** — every operator query goes through `GetReservation` over OCPP. **Rejected**: there's no `GetReservation` RPC in OCPP 1.6 (the charger doesn't expose its reservation state to the CSMS via a query — only via the `Reserved` connector status). Operator dashboards listing "reservations across the fleet" would need to fan out one OCPP query per charger. The mirror gives O(1) reads.

- **Gateway-side scheduler that polls and ages out reservations** — a periodic `UPDATE reservations SET status = 'Expired' WHERE expiry_date < now() AND status = 'Active'`. **Rejected for v1**: needs a separate job process or a `pg_cron` extension. Computing the effective status at query time (`CASE WHEN now() > expiry_date THEN 'Expired' ELSE status END`) gives the same answer with no infra. We can revisit if/when reservation-rate analytics need a settled column.

## Consequences

### Positive

- Operator dashboards / mobile BFF can render reservations without OCPP round-trips.
- The "charger is the truth, gateway mirrors it" mental model is consistent across LocalAuthList (E2-1B), Reservations (this), and the eventual Smart Charging persistence (E2-1E will use the same shape).
- Persist-only-on-Accepted means a charger that rejects an operator request never leaves a half-state row that confuses later queries.

### Negative / costs

- **Pending row written before charger reply.** A gateway crash between the `INSERT ... RETURNING` and the OCPP round-trip leaves a `Pending` row that no charger ever Accepted. Mitigated by a sweep query that deletes `status='Pending' AND created_at < now() - interval '1 hour'`. Not implemented today (zero observed traffic on this path); easy to add when Phase 4 load test exposes the rate.
- **Stale rows after charger reboot.** If a charger reboots and forgets its local reservations, the gateway mirror still says "Active". Mitigated when the next operator action — `CancelReservation` against a forgotten reservation — gets `Rejected` from the charger; the row stays `Active` in the mirror, but a follow-up `GetChargerStatus` will show the connector as `Available`, and the operator can ignore the stale reservation row. Phase 4/5 may add a charger-boot-time reconciliation, but that's a real subsystem and out of scope for E2-1C.
- **No FK from `reservations.transaction_id` to `transactions.id`.** v1 doesn't link a reservation to its consuming transaction; analytics that want "which reservations got used" need a join on `(charge_point_id, id_tag, time-window)`. Acceptable for v1; explicit in this ADR so reviewers don't assume we have it.

### Risks

- **Two clocks for expiry.** Charger uses its local clock; gateway uses Postgres `now()`. If they drift far enough, a reservation that the gateway thinks is still active is in fact already consumed (or vice versa). Mitigation: the AGENTS rule already says charger clocks are untrusted. Operator queries should treat `expiry_date < now()` as definitively expired regardless of `status` — if a charger says otherwise, the charger is wrong.
- **Pending-row leak under sustained crash storms.** If the gateway repeatedly crashes after `INSERT ... RETURNING` but before the charger reply, `reservations` accumulates `Pending` rows. The sweep query (described under "costs") is the answer; until it's implemented, ops monitoring should alert on `count(*) WHERE status='Pending' AND created_at < now() - interval '1 hour' > N`.

### Reversibility

Reversible. Switching to CSMS-side authority would require: (a) adding pre-flight validation in `ReserveNow` (Postgres read against `reservations` + the registry), (b) adding a per-charger expiry scheduler, and (c) reconciling the stored `status` against charger replies on every operator action. Estimated migration: ~3 days of work plus integration tests. The data model itself doesn't change much — `reservations` is the same shape either way; only the order of operations on the dispatch path differs.

## References

- [ADR-0017](./0017-idempotency-cache.md) — the same "charger / external truth, gateway is best-effort cache" pattern for inbound replays.
- [ADR-0020](./0020-clickhouse-ingestion-sidecar.md) — analogous "events flow one-way; gateway doesn't read from ClickHouse" rule.
- [`docs/02-tasks.md`](../02-tasks.md) — E2-1C row.
- OCPP 1.6 Edition 2 §6.40 (`ReserveNow.req`), §6.41 (`ReserveNow.conf`), §6.7 (`CancelReservation.req`), §6.8 (`CancelReservation.conf`).
