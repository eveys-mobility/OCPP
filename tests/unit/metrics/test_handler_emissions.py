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


# ---- Circuit breaker -----------------------------------------------------


@pytest.mark.asyncio
async def test_circuit_breaker_records_open_transition() -> None:
    """Two consecutive failures past the threshold trip the breaker
    and bump the transitions counter."""
    from eveys_ocpp.platform.circuit_breaker import CircuitBreaker

    breaker = CircuitBreaker(name="test_breaker_open", threshold=2, cooldown_seconds=60)

    before = _counter_sample(
        m.BACKEND_CIRCUIT_TRANSITIONS_TOTAL, name="test_breaker_open", to_state="open"
    )
    await breaker.record_failure()
    await breaker.record_failure()
    after = _counter_sample(
        m.BACKEND_CIRCUIT_TRANSITIONS_TOTAL, name="test_breaker_open", to_state="open"
    )
    assert after == before + 1


# ---- Kafka producer ------------------------------------------------------


@pytest.mark.asyncio
async def test_kafka_publish_records_ok_count_and_bytes() -> None:
    """KafkaEventProducer.publish bumps the ok counter and adds the
    byte count."""
    from eveys_ocpp.events import KafkaEventProducer

    producer = KafkaEventProducer(bootstrap_servers="ignored:0")
    fake = AsyncMock()
    producer._producer = fake  # type: ignore[assignment]

    before_count = _counter_sample(m.KAFKA_PUBLISH_TOTAL, topic="cp.boot", outcome="ok")
    before_bytes = _counter_sample(m.KAFKA_PUBLISH_BYTES_TOTAL, topic="cp.boot")

    await producer.publish(topic="cp.boot", key="CP_X", value=b"\x00\x01\x02")

    assert _counter_sample(m.KAFKA_PUBLISH_TOTAL, topic="cp.boot", outcome="ok") == (
        before_count + 1
    )
    assert _counter_sample(m.KAFKA_PUBLISH_BYTES_TOTAL, topic="cp.boot") == before_bytes + 3


# ---- Idempotency cache ----------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_increments_miss_and_replay() -> None:
    """First call is a miss, second is a replay — both bump the
    matching outcome label."""
    from eveys_ocpp.idempotency import IdempotencyCache
    from eveys_ocpp.settings import Settings

    fake_redis = MagicMock()
    # First write returns truthy (key created); second returns None
    # because the key already exists.
    fake_redis.set = AsyncMock(side_effect=[True, None])

    cache = IdempotencyCache(redis=fake_redis, settings=Settings())

    miss_before = _counter_sample(m.IDEMPOTENCY_LOOKUPS_TOTAL, outcome="miss")
    replay_before = _counter_sample(m.IDEMPOTENCY_LOOKUPS_TOTAL, outcome="replay")

    seen1 = await cache.check_and_record(cp_id="CP_X", message_id="m-1")
    seen2 = await cache.check_and_record(cp_id="CP_X", message_id="m-1")

    assert seen1 is False
    assert seen2 is True
    assert _counter_sample(m.IDEMPOTENCY_LOOKUPS_TOTAL, outcome="miss") == miss_before + 1
    assert _counter_sample(m.IDEMPOTENCY_LOOKUPS_TOTAL, outcome="replay") == replay_before + 1


# ---- DB query latency histogram -------------------------------------------


def test_classify_op_buckets_known_verbs() -> None:
    """Bounded enum — anything we don't recognise lands in `other`."""
    from eveys_ocpp.persistence.db import _classify_op

    assert _classify_op("SELECT * FROM x") == "select"
    assert _classify_op("  insert into x") == "insert"
    assert _classify_op("UPDATE x SET y=1") == "update"
    assert _classify_op("DELETE FROM x") == "delete"
    assert _classify_op("CREATE TABLE x()") == "other"
    assert _classify_op("") == "other"
