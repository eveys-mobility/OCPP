"""Tests for the webhook dispatcher's per-event delivery + retry math.

The dispatcher's Kafka-consumer loop isn't unit-tested here — it
needs a real broker, exercised by the e2e tier later. What's tested:

- envelope → wire body translation (`_build_body`)
- URL routing per event type (`_url_for`)
- enabled-topics surface (`_enabled_topics`)
- per-attempt retry behaviour against a mocked HTTP client
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher


def _settings(**overrides: Any) -> Settings:
    base = {
        "webhook_base_url": "https://backend.example/webhooks",
        "webhook_secret": "shared-secret",
    }
    base.update(overrides)
    # `_env_file=None` so a developer's local `.env` (which may set
    # `webhook_url_cp_boot=...` overrides for their dev backend) doesn't
    # leak in and fail this test. CI has no `.env` checked in, so this
    # only manifests on local laptops; the explicit None makes the test
    # behave the same in both environments.
    return Settings(_env_file=None, **base)


def _envelope(payload_kind: str, **fields: Any) -> events_pb2.EventEnvelope:
    """Build a minimal `EventEnvelope` for tests."""
    env = events_pb2.EventEnvelope()
    env.event_id = fields.pop("event_id", "evt-test")
    env.occurred_at = fields.pop("occurred_at", "2026-05-07T12:00:00+00:00")
    env.cp_id = fields.pop("cp_id", "CP_TEST")
    if payload_kind == "cp_boot":
        b = env.cp_boot
        b.vendor = fields.pop("vendor", "ACME")
        b.model = fields.pop("model", "X1")
        b.firmware_version = fields.pop("firmware_version", "1.0.0")
        b.serial_number = fields.pop("serial_number", "SN-1")
        b.status = fields.pop("status", events_pb2.CP_BOOT_STATUS_ACCEPTED)
    elif payload_kind == "cp_status":
        s = env.cp_status
        s.connector_id = fields.pop("connector_id", 1)
        s.status = fields.pop("status", "Charging")
        s.error_code = fields.pop("error_code", "NoError")
    elif payload_kind == "tx_started":
        t = env.tx_started
        t.connector_id = fields.pop("connector_id", 1)
        t.transaction_id = fields.pop("transaction_id", 12345)
        t.id_tag = fields.pop("id_tag", "RFID_X")
        t.meter_start_wh = fields.pop("meter_start_wh", 1_000_000)
    return env


# ---- _build_body -----------------------------------------------------------


def test_build_body_cp_boot_shape() -> None:
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("cp_boot"))
    assert body is not None
    assert body["success"] is True
    assert body["message"] == "cp.boot"
    data = body["data"]
    assert data["event_type"] == "cp.boot"
    assert data["cp_id"] == "CP_TEST"
    assert data["vendor"] == "ACME"
    assert data["registration_status"] == "Accepted"


def test_build_body_cp_status_shape() -> None:
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("cp_status", connector_id=2, status="Faulted"))
    assert body is not None
    assert body["data"]["event_type"] == "cp.status_changed"
    assert body["data"]["connector_id"] == 2
    assert body["data"]["status"] == "Faulted"


def test_build_body_tx_started_shape() -> None:
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("tx_started"))
    assert body is not None
    assert body["data"]["event_type"] == "tx.started"
    assert body["data"]["transaction_id"] == 12345


def test_build_body_returns_none_for_disabled_event() -> None:
    """`webhook_enable_cp_boot=False` → cp.boot skipped silently."""
    d = WebhookDispatcher(_settings(webhook_enable_cp_boot=False))
    assert d._build_body(_envelope("cp_boot")) is None


def test_build_body_returns_none_for_cp_meter_default() -> None:
    """cp.meter is off by default per the spec."""
    d = WebhookDispatcher(_settings())  # webhook_enable_cp_meter defaults to False
    env = events_pb2.EventEnvelope()
    env.event_id = "e"
    env.cp_id = "CP_TEST"
    env.cp_meter.connector_id = 1
    assert d._build_body(env) is None


# ---- _url_for --------------------------------------------------------------


def test_url_for_uses_per_event_override_when_set() -> None:
    d = WebhookDispatcher(_settings(webhook_url_cp_boot="https://override.example/boot"))
    assert d._url_for(_envelope("cp_boot")) == "https://override.example/boot"


def test_url_for_falls_back_to_base() -> None:
    d = WebhookDispatcher(_settings())
    assert d._url_for(_envelope("cp_boot")) == "https://backend.example/webhooks/cp-boot"
    assert (
        d._url_for(_envelope("cp_status")) == "https://backend.example/webhooks/cp-status-changed"
    )


def test_url_for_returns_none_for_disabled_event() -> None:
    d = WebhookDispatcher(_settings(webhook_enable_cp_status=False))
    assert d._url_for(_envelope("cp_status")) is None


# ---- _enabled_topics -------------------------------------------------------


def test_enabled_topics_default() -> None:
    d = WebhookDispatcher(_settings())
    topics = d._enabled_topics()
    # Default: boot, status, tx-started enabled; meter off.
    assert "cp.boot" in topics
    assert "cp.status" in topics
    assert "tx.started" in topics
    assert "cp.meter" not in topics


def test_enabled_topics_all_off() -> None:
    d = WebhookDispatcher(
        _settings(
            webhook_enable_cp_boot=False,
            webhook_enable_cp_status=False,
            webhook_enable_tx_started=False,
        )
    )
    assert d._enabled_topics() == ()


# ---- _post_with_retry ------------------------------------------------------


def _resp(status: int) -> MagicMock:
    r = MagicMock(spec=httpx.Response)
    r.status_code = status
    r.text = ""
    return r


@pytest.mark.asyncio
async def test_post_succeeds_first_try() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=5))
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(200))

    await d._post_with_retry(
        url="https://x/test",
        body_bytes=b"{}",
        signature="sha256=abc",
        event_id="evt-1",
        event_type="cp.boot",
    )

    assert d._http.post.await_count == 1


@pytest.mark.asyncio
async def test_post_retries_on_5xx_then_succeeds() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=3))
    d._http = MagicMock()
    d._http.post = AsyncMock(side_effect=[_resp(503), _resp(503), _resp(200)])

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-2",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 3


@pytest.mark.asyncio
async def test_post_does_not_retry_on_4xx() -> None:
    """400 / 401 / 403 / 404: backend rejected — retry would be wasted."""
    d = WebhookDispatcher(_settings(webhook_max_attempts=5))
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(401))

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-3",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 1


@pytest.mark.asyncio
async def test_post_retries_on_429_unlike_other_4xx() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=2))
    d._http = MagicMock()
    d._http.post = AsyncMock(side_effect=[_resp(429), _resp(200)])

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-4",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 2


@pytest.mark.asyncio
async def test_post_gives_up_after_max_attempts() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=3))
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(503))

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-5",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 3


@pytest.mark.asyncio
async def test_post_retries_on_network_error() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=2))
    d._http = MagicMock()
    d._http.post = AsyncMock(side_effect=[httpx.ConnectError("refused"), _resp(200)])

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-6",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 2


@pytest.mark.asyncio
async def test_post_retries_on_timeout() -> None:
    d = WebhookDispatcher(_settings(webhook_max_attempts=2))
    d._http = MagicMock()
    d._http.post = AsyncMock(side_effect=[httpx.ReadTimeout("slow"), _resp(200)])

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-7",
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 2
