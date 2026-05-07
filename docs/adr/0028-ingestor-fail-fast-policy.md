# ADR-0028: ClickHouse ingestor fail-fast on sustained INSERT failure

| Status | Date | Authors |
|---|---|---|
| Accepted | 2026-05-07 | Mostafa |

## Context

The ClickHouse ingestor sidecar (ADR-0020) reads protobuf envelopes
from Kafka and INSERTs them into the matching `cp_*` / `tx_*` tables.
The hot loop is poll → process → flush → commit; offsets are committed
manually only after a successful flush so a crash mid-batch
re-delivers (at-least-once).

The original `_flush_batch` failure path simply logged the exception
and slept 1 s before re-polling. That works for transient blips
(garbage collection pause, brief network jitter, a single timeout)
because the next poll re-delivers and the next INSERT lands.

It does not work for **wedged** failures — schema missing, ingestor
pointed at the wrong CH instance, type mismatch on a recently-evolved
column. In every one of those cases:

- Every flush raises forever; the consumer-group offset never moves.
- Fresh events keep landing in the Kafka topic but never reach
  ClickHouse, so reads return stale or empty data.
- The container is marked `Up` and `healthy` (we don't probe CH
  readiness from the ingestor's container healthcheck).
- The only signal is a log line per failure, growing without bound,
  with no human actively tailing it.

We hit this exact failure during the issue #24 investigation: a
Homebrew CH on `localhost:8123` collided with the docker CH on the
same port, the migrations went to the wrong server, and the docker CH
stayed empty. The ingestor logged `clickhouse.ingestor.flush_failed`
roughly twice a second for hours before anyone noticed — the gateway
was returning empty `connectors[]` arrays the whole time.

A misconfiguration that doesn't heal on its own should not present as
a green container.

## Decision

The ingestor tracks consecutive `_flush_batch` failures and **raises
`IngestorFatalError`** once the count reaches a configurable
threshold. The exception propagates to `main()`, which logs a single
exit line and returns exit code 1. Docker compose / kubernetes treat
that as a crash and restart the container, surfacing the wedge as a
`CrashLoopBackOff` (or compose's equivalent) instead of a silent loop.

The counter resets on every successful flush, so the policy only
catches *sustained* failure — not the occasional transient one.

The threshold is `EVEYS_OCPP_CLICKHOUSE_INGESTOR_MAX_FLUSH_FAILURES`,
defaulting to **10**.

## Alternatives considered

- **Liveness probe that fails when the failure counter is non-zero.**
  Rejected: any single transient failure would mark the pod unhealthy
  and Kubernetes would restart it, defeating the "ride out a CH GC
  pause" property. The probe approach also doesn't help docker compose
  on a laptop, which has no built-in liveness concept beyond the
  process exit code.

- **Bail on the first failure.** Rejected: a one-shot CH timeout or a
  brief broker disconnect should not crash the process. The cost of a
  pod restart isn't zero — Kafka rebalance, partition reassignment,
  cold caches — and the loop already retries cleanly on the next poll.

- **Bail by elapsed time, not count.** Rejected as more complex with
  no practical advantage. Counting failures while keeping the existing
  fixed 1 s backoff means "10 consecutive failures" maps to roughly
  10–60 seconds of dead air (depending on `BATCH_MAX_SECONDS`) — a
  range an operator would already consider unacceptable.

- **Surface the wedge via Prometheus / alertmanager only.** Rejected
  as the *only* mechanism. We do want the metric (E4-1's
  `eveys_ocpp_ingestor_flush_failures_total`), but a metric with no
  alert is invisible, an alert depends on alertmanager being healthy,
  and neither helps the laptop developer who hasn't wired up
  monitoring. Process-exit is the universally-observable signal.

## Consequences

### Positive

- Wedged misconfigurations show up as restart loops within ~1 minute
  (10 failures × ~5 s batch window). Operators see `CrashLoopBackOff`
  with the cause in the last log lines.
- Single transient failures still ride through silently — counter
  resets on the next successful flush.
- No new dependencies. The whole change is one counter, one raise,
  and one `sys.exit(1)` in `main()`.

### Negative / costs

- A network-partition that lasts longer than ~1 minute now restarts
  the ingestor instead of waiting it out. Restart is cheap (a few
  seconds) but it does mean we'll briefly stop ingesting on the
  margin of "should this even count as down?" That tradeoff is fine
  for our scale; revisit if Kafka-rebalance cost ever dominates.
- The default of 10 is a guess. Easy to tune via env var if the
  in-the-field rate of false-positive bails turns out to be too
  spicy.

### Risks

- A failure that flips between flushed and failed (e.g. one CH
  replica out of three returning errors) would never trip the limit
  even if half the writes fail. The counter is "consecutive", not
  "rolling N". Acceptable: that scenario is already abnormal and the
  per-attempt log carries the error code, but it's worth knowing the
  policy isn't a substitute for a metric-based alert.

### Reversibility

- Reversible. Setting
  `EVEYS_OCPP_CLICKHOUSE_INGESTOR_MAX_FLUSH_FAILURES` to a very large
  number (e.g. `1000000`) effectively restores the old log-and-loop
  behaviour without code changes. Removing the policy entirely is a
  ~10-line revert.

## References

- ADR-0020 — ClickHouse ingestion sidecar (the loop the policy lives in).
- Issue #24 — the wedge that motivated the policy.
- PR #26 — implementation + tests.
- `src/eveys_ocpp/clickhouse/ingestor.py` — `IngestorFatalError`,
  the counter, and the bail in `ingest_loop`.
- `docs/07-local-dev-setup.md` § Troubleshooting — operator-facing
  "what to do when the ingestor crashlooops" guidance.
