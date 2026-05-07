"""Tiny in-house async circuit breaker for the backend HTTP client.

State machine (per breaker instance):

- **closed** — calls flow through. Each failure increments a counter;
  ``threshold`` consecutive failures → trip to *open*.
- **open** — calls short-circuit immediately with
  ``BackendCircuitOpenError`` until ``cooldown_seconds`` elapses, then
  → half-open.
- **half-open** — one probe call is allowed. Success → *closed* (counter
  reset). Failure → *open* (cooldown restarts).

Counter reset rules: any successful call in *closed* resets the failure
counter to 0. Failure in *closed* increments. Failure in *half-open*
goes back to *open*.

Thread safety: not used; the gateway is single-process asyncio. A
single `asyncio.Lock` guards state mutations across coroutines.

Why not use `pybreaker` / `aiocircuitbreaker`: both are reasonable
libraries, but the breaker is ~30 lines and adding a runtime dep
for that violates AGENTS rule 4 ("no runtime deps without
justification"). If breaker semantics ever get more complex (e.g.
per-error-class counters, multi-state observability), revisit.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.platform.errors import BackendCircuitOpenError

log = get_logger(__name__)


class _State(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreaker:
    """Per-endpoint-group breaker. Construct one per logical scope
    (e.g. one for the whole backend client is fine for v1; finer
    grained later if needed).
    """

    name: str
    threshold: int
    cooldown_seconds: float
    _state: _State = _State.CLOSED
    _consecutive_failures: int = 0
    _opened_at: float = 0.0

    def __post_init__(self) -> None:
        # Lock isn't a dataclass field so the dataclass stays
        # equality-comparable in tests; created lazily.
        self._lock = asyncio.Lock()

    async def before_call(self) -> None:
        """Raise ``BackendCircuitOpenError`` if the breaker is open
        and the cooldown hasn't elapsed. Promotes open → half-open
        when the cooldown is up."""
        async with self._lock:
            if self._state is _State.OPEN:
                if time.monotonic() - self._opened_at >= self.cooldown_seconds:
                    log.info("circuit_breaker.half_open", name=self.name)
                    self._state = _State.HALF_OPEN
                    self._record_state_metric()
                    metrics_registry.BACKEND_CIRCUIT_TRANSITIONS_TOTAL.labels(
                        name=self.name, to_state="half_open"
                    ).inc()
                else:
                    raise BackendCircuitOpenError(f"circuit breaker open for {self.name}")

    async def record_success(self) -> None:
        async with self._lock:
            if self._state is not _State.CLOSED:
                log.info("circuit_breaker.closed", name=self.name)
                metrics_registry.BACKEND_CIRCUIT_TRANSITIONS_TOTAL.labels(
                    name=self.name, to_state="closed"
                ).inc()
            self._state = _State.CLOSED
            self._consecutive_failures = 0
            self._record_state_metric()

    async def record_failure(self) -> None:
        async with self._lock:
            self._consecutive_failures += 1
            # A failure in half-open immediately re-opens. A failure
            # in closed trips when threshold is reached.
            should_open = self._state is _State.HALF_OPEN or (
                self._state is _State.CLOSED and self._consecutive_failures >= self.threshold
            )
            if should_open:
                # Always coming from CLOSED or HALF_OPEN here — either way
                # this is a transition to OPEN, so log it unconditionally.
                log.warning(
                    "circuit_breaker.opened",
                    name=self.name,
                    consecutive_failures=self._consecutive_failures,
                )
                self._state = _State.OPEN
                self._opened_at = time.monotonic()
                self._record_state_metric()
                metrics_registry.BACKEND_CIRCUIT_TRANSITIONS_TOTAL.labels(
                    name=self.name, to_state="open"
                ).inc()

    def _record_state_metric(self) -> None:
        """Reflect the live state into the gauge. closed=0, half_open=1, open=2."""
        value = {_State.CLOSED: 0.0, _State.HALF_OPEN: 1.0, _State.OPEN: 2.0}[self._state]
        metrics_registry.BACKEND_CIRCUIT_STATE.labels(name=self.name).set(value)

    @property
    def state(self) -> str:
        """Read-only state name for tests / observability."""
        return self._state.value

    @property
    def consecutive_failures(self) -> int:
        return self._consecutive_failures
