"""Shared drain state for graceful shutdown.

The gateway sits behind a sticky load balancer (Envoy with
consistent-hash on `cp_id`, see `docs/00-overview.md`). When a pod
is shutting down — rolling deploy, scale-down, or operator-driven
restart — three things have to happen in order or chargers see
brief connection refusals:

1. The pod's readiness probe must fail so the LB removes it from
   rotation. New WS upgrades land on a sibling pod instead.
2. Wait long enough for the LB to actually notice. k8s typically
   polls every few seconds with a small failure threshold.
3. Then tear down: cancel the TaskGroup, close servers, flush
   Kafka producer, close Redis, flush spans.

`DrainController` is the in-process flag that drives step 1. It is
built once in `_serve_all` and shared with:

- the REST app — `GET /api/v1/ready` returns 503 when draining,
  which is the signal the LB's readiness probe reads.
- the SIGTERM handler — flips the flag and orchestrates the wait
  before cancelling the TaskGroup.

Why a small dedicated module rather than a flag in `Settings`:
`Settings` is frozen (ADR-0001) and represents static config.
Drain state is mutable lifecycle and must change at runtime.
"""

from __future__ import annotations

import time

from eveys_ocpp.observability import get_logger

log = get_logger(__name__)


class DrainController:
    """Single source of truth for whether this pod is draining.

    The flag is one-way: once `begin_drain` is called the pod is
    committing to shutdown. There is no `end_drain`; recovery from
    a drain mistake is a pod restart.
    """

    def __init__(self) -> None:
        self._draining = False
        self._drain_started_at: float | None = None

    @property
    def is_draining(self) -> bool:
        return self._draining

    @property
    def drain_started_at(self) -> float | None:
        """Monotonic timestamp of the drain trigger, or None.

        Useful for tests and for the readiness endpoint to report
        how long the pod has been draining."""
        return self._drain_started_at

    def begin_drain(self) -> None:
        """Flip the draining flag. Safe to call more than once;
        repeated calls are no-ops so a duplicate signal (e.g. a
        second SIGTERM from an impatient operator) doesn't reset
        the start timestamp.
        """
        if self._draining:
            return
        self._draining = True
        self._drain_started_at = time.monotonic()
        log.info("drain.begin")
