# Service-level objectives (SLOs)

The five promises the gateway makes about its production behaviour. Each SLO is the **operator-facing** name; the SLI is the actual metric expression; the target is what we commit to; the window is over which interval; the consequence is what happens on breach.

> Recording rules in [`deploy/prometheus/rules.yml`](../deploy/prometheus/rules.yml). Compliance dashboard in [`deploy/grafana/dashboards/06-slos.json`](../deploy/grafana/dashboards/06-slos.json) (Grafana UID `eveys-ocpp-slos`).

> **Alerting on SLO breach is intentionally NOT wired** in Phase 4. Alertmanager + paging integration is deferred until on-call is staffed (Phase 7). The "consequence" column below describes what *should* fire when alerting lands; today the dashboards are the only signal.

## Error budgets — quick reference

| # | SLO | Target | Window | Error budget per window |
| - | --- | --- | --- | --- |
| 1 | Charger connection availability | ≥ 99.5% | 30d | ~3.6 hours of refused boots |
| 2 | Authorize latency | ≤ 500ms (P95) | 7d | 5% of authorizes may exceed 500ms |
| 3 | RemoteStart end-to-end | ≤ 3s (P95) | 7d | 5% of RemoteStarts may exceed 3s |
| 4 | Transaction durability | = 100% | 30d | 0% — zero-tolerance |
| 5 | Webhook delivery success | ≥ 99% | 7d | 1% — retries should absorb most hiccups |

---

## SLO 1 — Charger connection availability

The gateway exists to accept charger connections. Every refused or crashed boot is a charger that's offline for billing.

**SLI** — fraction of `BootNotification` decisions the gateway returned as `Accepted`:
```promql
sum(rate(eveys_ocpp_boot_notifications_total{decision="Accepted"}[30d]))
/
sum(rate(eveys_ocpp_boot_notifications_total[30d]))
```
Recorded as `slo:boot_acceptance:ratio_30d`.

**Target** — `≥ 99.5%`.

**Window** — rolling 30 days.

**Error budget** — `0.5%` over 30 days. At the rough scale of 10k chargers each booting ~once per day, that's ~1,500 acceptable refusals per month, or roughly 3.6 hours of zero-acceptance time.

**Consequence on breach (Phase 7)** — page on-call. Common causes: backend `register` endpoint is failing (check `eveys_ocpp_backend_circuit_state{endpoint="charge_points_register"}`), Postgres pool exhausted, idempotency cache wiped (Redis flushed).

---

## SLO 2 — Authorize latency

RFID swipes are user-facing. Anything over 500ms feels like the charger is broken — drivers tap again, generating a duplicate Authorize.

**SLI** — P95 of the backend's `/authorize` HTTP call, including circuit-breaker overhead:
```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(eveys_ocpp_backend_request_latency_seconds_bucket{endpoint="authorize"}[7d])
  )
)
```
Recorded as `slo:authorize_latency:p95_7d`.

**Target** — `≤ 500ms`.

**Window** — rolling 7 days. Shorter than SLO 1 because user-facing latency regressions need to surface quickly.

**Error budget** — 5% of authorizes are allowed to exceed 500ms.

**Consequence on breach (Phase 7)** — page on-call. The Authorize cache (E3-4) is the load shock-absorber; if its hit rate drops (`eveys_ocpp_authorize_cache_hits_total / (eveys_ocpp_authorize_cache_hits_total + eveys_ocpp_authorize_cache_misses_total)`), the SLO will follow. Verify backend latency upstream before assuming the cache is at fault.

---

## SLO 3 — RemoteStart end-to-end

Matches the load-test pass criterion (E4-6, [docs/13-load-testing.md](./13-load-testing.md)).

**SLI** — P95 of the gRPC `RemoteStart` RPC's wall-clock latency. The histogram already includes the OCPP CALL round-trip to the charger, so this measures the user-visible "tap RemoteStart, see charger respond" timeline:
```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(eveys_ocpp_grpc_request_latency_seconds_bucket{rpc="RemoteStart"}[7d])
  )
)
```
Recorded as `slo:remote_start_latency:p95_7d`.

**Target** — `≤ 3s`.

**Window** — rolling 7 days.

**Error budget** — 5% of RemoteStarts may exceed 3s.

**Consequence on breach (Phase 7)** — page on-call. RemoteStart latency is dominated by the charger's response time, but the gateway can compound it via the cross-pod bus path (`eveys_ocpp_bus_request_latency_seconds`) and Postgres look-ups (`eveys_ocpp_db_query_latency_seconds{op="select"}`). Drill in via the Per-pod dashboard.

---

## SLO 4 — Transaction durability

Money flows on this. Any StopTransaction we received but didn't persist is a billing incident.

**SLI** — fraction of received StopTransactions that landed in Postgres:
```promql
sum(rate(eveys_ocpp_stop_transactions_total[30d]))
/
(
  sum(rate(eveys_ocpp_stop_transactions_total[30d]))
  +
  sum(rate(eveys_ocpp_handler_errors_total{action="StopTransaction"}[30d]))
)
```
Recorded as `slo:transaction_durability:ratio_30d`.

**Target** — `= 100%`. Zero-tolerance.

**Window** — rolling 30 days.

**Error budget** — `0%`. Every drop in this number is a postmortem.

**Caveat (current implementation)** — we don't currently emit a separate "received but not persisted" counter. `eveys_ocpp_stop_transactions_total` is incremented after the handler's DB commit, so a stop that fails to persist gets counted in `eveys_ocpp_handler_errors_total{action="StopTransaction"}` instead. The SLI sums both as the denominator and only the persisted total as the numerator. **If a future task adds a dedicated `_received_total` counter, switch the denominator to that** — the recording rule has a comment pointing here.

**Consequence on breach (Phase 7)** — wake the on-call engineer regardless of hour. Investigate via Sentry (E4-4) for the StopTransaction handler exception; cross-reference the `transaction_id` in OCPP charger replays via `eveys_ocpp_stop_transaction_replays_total` (a healthy idempotency cache absorbs most retries).

---

## SLO 5 — Webhook delivery success

The webhook dispatcher is how the backend learns about gateway events. Its retry logic should absorb most transient hiccups; if it can't, the backend's view of the world drifts.

**SLI** — fraction of envelopes the dispatcher successfully delivered, **excluding** `rejected` (4xx — the backend's bug, not ours):
```promql
sum(rate(eveys_ocpp_webhook_deliveries_total{outcome="delivered"}[7d]))
/
sum(rate(eveys_ocpp_webhook_deliveries_total{outcome=~"delivered|failed"}[7d]))
```
Recorded as `slo:webhook_delivery:ratio_7d`.

**Target** — `≥ 99%`.

**Window** — rolling 7 days.

**Error budget** — 1% of envelopes may end up `failed` (retry budget exhausted) over a week.

**Consequence on breach (Phase 7)** — page on-call. Common cause: backend webhook endpoint is failing (check the backend's own monitoring) or our retry budget is too tight. Look at `eveys_ocpp_webhook_attempts_total` — if many envelopes are exhausting their retry budget, raise it; if attempts are flat-lining, the dispatcher itself is wedged.

---

## What's intentionally NOT here

- **Alerting on SLO breach.** Alertmanager + paging integration is its own task — deferred until on-call is staffed (Phase 7). The dashboard panels go yellow/red on breach but nothing pages.
- **SLAs (the customer-facing version).** SLAs have contractual teeth and a different audience. Different document, different review path.
- **Per-tenant SLO slicing.** All five SLOs are fleet-wide; there's no tenant dimension. Phase 6+ work if multi-tenant isolation lands.

## Loading the recording rules

Mount `deploy/prometheus/rules.yml` into your Prometheus container alongside its main config and reference it under `rule_files:`:

```yaml
# prometheus.yml
rule_files:
  - /etc/prometheus/rules/eveys-ocpp.yml
```

```yaml
# docker-compose.yml additions for the operator's own Prometheus stack
services:
  prometheus:
    volumes:
      - ./deploy/prometheus/rules.yml:/etc/prometheus/rules/eveys-ocpp.yml:ro
```

The rules evaluate every 30 seconds (per the `interval:` field at the top of the group). Reload with `kill -HUP <prometheus-pid>` or `curl -X POST http://prometheus:9090/-/reload` after editing.
