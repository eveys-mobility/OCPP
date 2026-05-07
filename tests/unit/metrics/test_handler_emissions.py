"""Per-emitter delta tests for the hot-path metrics.

Pattern: read each metric's sample value before / after the call,
assert the delta. Tolerates label additions in future PRs because
each test pins to the exact label set it observed.

The metrics live on the global `prometheus_client.REGISTRY` and
persist across tests in the same process. Assert on deltas, not
absolutes — order-independent and immune to other tests bumping the
same counter elsewhere in the suite.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from eveys_ocpp.metrics import registry as m


def _counter_sample(metric: Any, **labels: str) -> float:
    """Read the wire-format `_total` sample value for a labelled
    Counter, returning 0.0 if the label set hasn't been touched yet.

    We collect on the parent metric (not the labelled child) because
    `labelled.collect()` strips the label dict on every sample;
    only the parent's `collect()` returns each child series with its
    full label tuple.
    """
    target = labels or {}
    for family in metric.collect():
        for sample in family.samples:
            if not sample.name.endswith("_total"):
                continue
            if sample.labels == target:
                return float(sample.value)
    return 0.0


# ---- Heartbeat -----------------------------------------------------------


@pytest.mark.asyncio
async def test_heartbeat_increments_counter_on_success(
    fake_cp: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.handlers.v16 import heartbeat

    # Stub out the DB write so the handler runs without a real session.
    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())

    before = _counter_sample(m.HEARTBEATS_TOTAL)
    await heartbeat.handle(fake_cp)
    after = _counter_sample(m.HEARTBEATS_TOTAL)
    assert after == before + 1


@pytest.mark.asyncio
async def test_heartbeat_records_registry_reclaim_when_key_expired(
    fake_cp: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A heartbeat that finds the registry key gone re-claims and
    bumps the reclaim counter once."""
    from eveys_ocpp.handlers.v16 import heartbeat

    monkeypatch.setattr(heartbeat, "update_heartbeat", AsyncMock())
    fake_cp.registry = MagicMock()
    fake_cp.registry.refresh = AsyncMock(return_value=False)
    fake_cp.registry.mark_online = AsyncMock()

    before = _counter_sample(m.HEARTBEAT_REGISTRY_RECLAIMS_TOTAL)
    await heartbeat.handle(fake_cp)
    after = _counter_sample(m.HEARTBEAT_REGISTRY_RECLAIMS_TOTAL)
    assert after == before + 1
    fake_cp.registry.mark_online.assert_awaited_once()


# ---- Authorize -----------------------------------------------------------


@pytest.mark.asyncio
async def test_authorize_no_backend_increments_offline_accepted(
    fake_cp: MagicMock,
) -> None:
    """W1/dev path (no backend wired) returns Accepted via the offline
    fallback bucket."""
    from eveys_ocpp.handlers.v16 import authorize

    fake_cp.backend_client = None
    fake_cp.authorize_cache = None

    before = _counter_sample(m.AUTHORIZE_TOTAL, decision="Accepted", source="offline")
    await authorize.handle(fake_cp, id_tag="RFID_X")
    after = _counter_sample(m.AUTHORIZE_TOTAL, decision="Accepted", source="offline")
    assert after == before + 1


# ---- BootNotification replay ---------------------------------------------


@pytest.mark.asyncio
async def test_boot_replay_increments_replay_and_decision_counters(
    fake_cp: MagicMock,
) -> None:
    """A replayed BootNotification (idempotency cache hit) bumps
    both the replay counter AND the decision counter (Accepted)."""
    from eveys_ocpp.handlers.v16 import boot_notification

    fake_cp.idempotency = MagicMock()
    fake_cp.idempotency.check_and_record = AsyncMock(return_value=True)
    fake_cp.event_producer = None
    fake_cp.backend_client = None

    before_replays = _counter_sample(m.BOOT_REPLAYS_TOTAL)
    before_accepted = _counter_sample(m.BOOT_NOTIFICATIONS_TOTAL, decision="Accepted")

    await boot_notification.handle(
        fake_cp,
        message_id="msg-replay-1",
        charge_point_vendor="ACME",
        charge_point_model="X1",
    )

    assert _counter_sample(m.BOOT_REPLAYS_TOTAL) == before_replays + 1
    assert _counter_sample(m.BOOT_NOTIFICATIONS_TOTAL, decision="Accepted") == (before_accepted + 1)


# ---- StatusNotification --------------------------------------------------


@pytest.mark.asyncio
async def test_status_notification_emits_status_and_error_label(
    fake_cp: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.handlers.v16 import status_notification

    monkeypatch.setattr(status_notification, "update_status", AsyncMock())

    before = _counter_sample(m.STATUS_NOTIFICATIONS_TOTAL, status="Charging", error_code="NoError")
    await status_notification.handle(
        fake_cp,
        connector_id=1,
        status="Charging",
        error_code="NoError",
    )
    after = _counter_sample(m.STATUS_NOTIFICATIONS_TOTAL, status="Charging", error_code="NoError")
    assert after == before + 1
