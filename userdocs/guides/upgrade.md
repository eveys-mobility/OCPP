# Upgrade

**Audience.** Operators planning a version bump.

**What this answers.** Versioning policy, supported upgrade paths, what's stable across releases, and how to roll forward without dropping charger connections.

> Rollback motions live in [`operate.md`](./operate.md) §5. This page is about going *forward*.

---

## 1. Versioning policy

The gateway uses **semantic versioning** for its Helm chart and Docker image: `MAJOR.MINOR.PATCH`.

- **MAJOR** — breaking changes to a stable surface (REST API, gRPC API, event envelope, Kafka topic names). Read the release notes carefully. Coordinate with backend / consumer teams before deploying.
- **MINOR** — additive changes. New endpoints, new event fields, new configuration knobs with safe defaults. Backward-compatible with the previous MINOR.
- **PATCH** — bug fixes, performance fixes, doc fixes. Always safe to apply.

The chart's `appVersion` matches the gateway's Docker image tag. The chart's own `version` increments independently — chart-only changes don't bump the app.

---

## 2. What's stable across releases

These surfaces follow the semver contract above. Treat them as the platform's contract with the rest of your stack.

| Surface | Stability rule |
|---|---|
| **REST API** | Existing endpoints, request shapes, and response shapes don't change inside a MAJOR. New fields may appear on responses; consumers must ignore unknown fields. New endpoints may be added at any time. |
| **gRPC API** | Existing RPCs, request and response messages, and proto field numbers don't change inside a MAJOR. New fields and RPCs are additive. Removing a field requires a MAJOR. |
| **Kafka event envelope** | `EventEnvelope` and every payload type follow proto3 evolution rules: adding fields is fine; renumbering or removing fields is forbidden until a MAJOR. Topic names don't change inside a MAJOR. |
| **Webhook payloads** | Same as the Kafka envelope (JSON serialisation of the same proto). Adding fields is fine; the headers (`X-Eveys-Signature`, `X-Eveys-Event-Type`, `X-Eveys-Event-Id`) are stable. |
| **OCPP wire format** | Defined by the OCPP standard, not by this project. The gateway never alters charger-facing payloads. |
| **`error_code` strings** | Stable enum-like surface. New codes may appear; existing codes never change meaning or get removed inside a MAJOR. |
| **Configuration env vars** | `EVEYS_OCPP_*` variables and their accepted values are stable inside a MAJOR. Deprecations are announced one MINOR in advance. |
| **Metric names** | `eveys_ocpp_*` series are stable. Adding new series is always allowed. Renaming or removing requires a MAJOR. |

What's **explicitly not** part of the contract: internal log fields beyond `level`, `event`, `request_id`, `cp_id`; trace span names; database schema (you should never query the DB directly — use REST); the proto field tag numbers inside the generated `*_pb2.py` (but the wire format they encode is stable).

---

## 3. The upgrade motion

### 3.1 Read the release notes

Every release ships with a `CHANGELOG.md` entry. Look for:

- **Schema changes.** Listed as `### Migration` in the changelog. A new Alembic revision means Postgres will be altered when the new pod starts.
- **Configuration changes.** Listed as `### Changed` or `### Removed`. New defaults can be surprising; review them.
- **Deprecations.** Listed as `### Deprecated`. Time to act before the next MAJOR.

### 3.2 Plan the order

For PATCH and most MINOR upgrades, the order is:

1. Update the chart values (image tag, new options).
2. `helm upgrade` against staging.
3. Watch metrics and logs for the soak period your platform standards require.
4. `helm upgrade` against production.

For a MAJOR upgrade, add two more steps:

0. **Coordinate with consumer teams** that any client-side changes are deployed first.
0. **Bench-test in a non-production cluster** end-to-end.

5. **Plan the rollback** ahead of time. Have the previous image tag and a known-good values file ready.

### 3.3 Run it

```bash
helm upgrade eveys-ocpp ./deploy/helm/eveys-ocpp \
  --namespace eveys-ocpp \
  --values eveys-ocpp.values.yaml \
  --version <chart-version>
```

What happens inside the chart:

1. The new gateway image rolls out one pod at a time, respecting `maxUnavailable: 0`.
2. Each new pod runs Alembic migrations on start (idempotent — no-op if already at HEAD).
3. The new pod fails its liveness probe until everything's wired; Kubernetes holds off until ready.
4. Old pods drain on `SIGTERM` per [`operate.md`](./operate.md) §4.3.

Watch:

```bash
kubectl -n eveys-ocpp rollout status deployment/eveys-ocpp-gateway --timeout=10m
```

---

## 4. Zero-downtime rules of thumb

- **`maxUnavailable: 0`** in the rolling-update strategy. Never below replica count.
- **`terminationGracePeriodSeconds`** ≥ `EVEYS_OCPP_SHUTDOWN_GRACE_PERIOD_SECONDS` + 5 s.
- **Postgres pool capacity** = `(2 × pod count × per-pod pool size)` during the rollout window — enough for both old and new pods to be checked out simultaneously.
- **Kafka producer backpressure** is built in; you don't need to size around the rollout.
- **Redis** sees a brief blip in registry traffic; this is normal and handled by the gateway's reconnect logic.

If you can't satisfy any of these, the upgrade will still work — it just won't be zero-downtime.

---

## 5. Schema migrations specifically

Migrations live in `alembic/versions/`. The gateway runs them automatically on pod start; you can also run them out-of-band with `make pg-migrate` against the DSN.

Migrations in this project are:

- **Additive by default.** New columns, new tables, new indexes — applied without downtime.
- **Backfill-safe.** When a migration backfills data, it's chunked so it doesn't lock the table for long.
- **Downward-compatible within a MINOR.** A new MINOR's schema works for the previous MINOR's code, so the rolling-update overlap is safe.
- **Reversible when reasonable.** Most have `downgrade()` paths; destructive migrations are called out in release notes.

If a migration takes longer than your `terminationGracePeriodSeconds`, the new pod fails its liveness probe. Two options:

1. Run the migration out-of-band first (`make pg-migrate`), then deploy the new image.
2. Raise the liveness probe's `initialDelaySeconds` and `failureThreshold` to give the migration time to finish on pod start.

---

## 6. Skipping versions

The gateway supports upgrading from any MINOR `X.Y.Z` to any MINOR `X.Y'.Z'` where `Y' >= Y` (and `X` matches). I.e., MINOR-to-MINOR upgrades inside one MAJOR are always safe.

You **cannot** skip a MAJOR. Going from `1.x` to `3.x` means stepping through `2.x` — usually because schema migrations or event envelope changes weren't designed to skip an intermediate MAJOR.

Downgrades are not supported across MINORs that introduced schema changes. See [`operate.md`](./operate.md) §5.2.

---

## 7. When NOT to upgrade

- During fleet peak hours. Even with zero-downtime rollouts, an upgrade is a perturbation. Off-peak is better.
- Inside a **freeze window** declared by your release process.
- While an incident is open. Upgrades during incidents add a variable to the post-mortem.
- When the release notes flag a known issue your environment matches.

For genuine **security patches**, treat the upgrade as part of incident response rather than scheduled work — different rules apply.

---

## Where to go from here

- Day-2 operations and rollback: [`operate.md`](./operate.md).
- What changed between versions: the project's `CHANGELOG.md` at the repo root.
- Configuration knobs: [`../reference/configuration.md`](../reference/configuration.md).
