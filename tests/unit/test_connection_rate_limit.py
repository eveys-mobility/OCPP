"""E5-3 wiring tests — route_message consults the rate limiter on
inbound CALLs and drops throttled frames before the OCPP library
sees them.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.connection import EveysChargePoint
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.settings import Settings


def _call_frame(action: str = "Heartbeat") -> str:
    """Build a minimal OCPP CALL frame: [2, message_id, action, payload]."""
    return json.dumps([2, "msg-123", action, {}])


def _result_frame() -> str:
    """Build an OCPP CALLRESULT frame: [3, message_id, payload]."""
    return json.dumps([3, "msg-123", {}])


def _make_cp(
    *,
    rate_limiter: object = None,
    settings: Settings | None = None,
) -> EveysChargePoint:
    connection = MagicMock()
    return EveysChargePoint(
        "TEST_CP_001",
        connection,
        session_factory=MagicMock(),
        settings=settings or Settings(),
        rate_limiter=rate_limiter,  # type: ignore[arg-type]
    )


@pytest.mark.asyncio
async def test_route_message_passes_through_when_no_limiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`rate_limiter=None` → handler runs as before, no throttling."""
    cp = _make_cp(rate_limiter=None)
    super_route = AsyncMock()
    monkeypatch.setattr("ocpp.v16.ChargePoint.route_message", super_route, raising=True)

    await cp.route_message(_call_frame("Heartbeat"))

    super_route.assert_awaited_once()


@pytest.mark.asyncio
async def test_route_message_drops_throttled_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = AsyncMock()
    limiter.check = AsyncMock(return_value=False)  # throttle every CALL
    limiter.record_throttled = AsyncMock()
    cp = _make_cp(rate_limiter=limiter)
    super_route = AsyncMock()
    monkeypatch.setattr("ocpp.v16.ChargePoint.route_message", super_route, raising=True)

    await cp.route_message(_call_frame("StatusNotification"))

    # The throttled CALL never reached the OCPP library.
    super_route.assert_not_awaited()
    limiter.check.assert_awaited_once_with("TEST_CP_001")
    limiter.record_throttled.assert_awaited_once_with(action="StatusNotification")


@pytest.mark.asyncio
async def test_route_message_lets_callresult_through_unchecked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CALLRESULT / CALLERROR are correlated responses to commands
    *we* sent — throttling them would break our own RemoteStart /
    Reset / etc. flows. The limiter must not be consulted for them."""
    limiter = AsyncMock()
    cp = _make_cp(rate_limiter=limiter)
    super_route = AsyncMock()
    monkeypatch.setattr("ocpp.v16.ChargePoint.route_message", super_route, raising=True)

    await cp.route_message(_result_frame())

    super_route.assert_awaited_once()
    limiter.check.assert_not_awaited()


@pytest.mark.asyncio
async def test_route_message_passes_call_through_when_limiter_allows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    limiter = AsyncMock()
    limiter.check = AsyncMock(return_value=True)
    cp = _make_cp(rate_limiter=limiter)
    super_route = AsyncMock()
    monkeypatch.setattr("ocpp.v16.ChargePoint.route_message", super_route, raising=True)

    await cp.route_message(_call_frame("Heartbeat"))

    super_route.assert_awaited_once()
    limiter.check.assert_awaited_once_with("TEST_CP_001")


@pytest.mark.asyncio
async def test_route_message_increments_counter_on_throttle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end of the wiring: a throttled frame increments the
    public metric, not just an internal counter."""
    # Real RateLimiter with a Redis stub that always denies.
    fake_redis = AsyncMock()
    fake_redis.eval = AsyncMock(return_value=[0, "0.0"])
    from eveys_ocpp.transport._rate_limiter import RateLimiter

    limiter = RateLimiter(fake_redis, settings=Settings())
    cp = _make_cp(rate_limiter=limiter)
    super_route = AsyncMock()
    monkeypatch.setattr("ocpp.v16.ChargePoint.route_message", super_route, raising=True)

    counter = metrics_registry.RATE_LIMIT_THROTTLED_TOTAL.labels(action="MeterValues")
    before = counter._value.get()

    await cp.route_message(_call_frame("MeterValues"))

    super_route.assert_not_awaited()
    assert counter._value.get() == before + 1
