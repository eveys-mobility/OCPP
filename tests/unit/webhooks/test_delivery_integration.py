"""End-to-end-shaped webhook delivery test, fully in-process.

Builds a real EventEnvelope, hands it to the dispatcher, captures the
HTTP request the dispatcher emits via httpx's `MockTransport`, and
verifies the signature on the receiving end. No Kafka involved — the
dispatcher's `_deliver_one` takes a `ConsumerRecord` directly so we
can drive it with a fake one.

This catches what the unit tests miss: the full envelope-to-
signed-HTTP-request pipeline, including JSON serialisation and the
header bundle. The Kafka consumer loop itself is e2e-tier work.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import httpx
import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher
from eveys_ocpp.webhooks.signer import verify_signature


@dataclass
class _FakeRecord:
    """Minimal stand-in for `aiokafka.ConsumerRecord`. Only the
    attributes the dispatcher reads (`value`, `topic`, `offset`)."""

    value: bytes
    topic: str = "cp.boot"
    offset: int = 0


def _settings(**overrides: Any) -> Settings:
    base = {
        "webhook_base_url": "https://backend.example/webhooks",
        "webhook_secret": "shared-secret",
    }
    base.update(overrides)
    # See test_dispatcher._settings for why `_env_file=None`.
    return Settings(_env_file=None, **base)


def _make_boot_envelope() -> events_pb2.EventEnvelope:
    env = events_pb2.EventEnvelope()
    env.event_id = "evt-int-001"
    env.occurred_at = "2026-05-07T14:00:00+00:00"
    env.cp_id = "CP_INT_001"
    env.cp_boot.vendor = "ACME"
    env.cp_boot.model = "X1"
    env.cp_boot.firmware_version = "2.0.0"
    env.cp_boot.serial_number = "SN-INT-1"
    env.cp_boot.status = events_pb2.CP_BOOT_STATUS_ACCEPTED
    return env


@pytest.mark.asyncio
async def test_envelope_delivers_signed_request_with_correct_headers() -> None:
    captured: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    s = _settings()
    d = WebhookDispatcher(s)
    # Skip start() — we don't need Kafka. Inject the mocked HTTP client.
    d._http = httpx.AsyncClient(transport=httpx.MockTransport(receiver))

    record = _FakeRecord(value=_make_boot_envelope().SerializeToString())
    await d._deliver_one(record)

    assert len(captured) == 1
    req = captured[0]

    # URL routed correctly — boot uses the per-event URL fallback.
    assert str(req.url) == "https://backend.example/webhooks/cp-boot"

    # Headers per docs/integration/03-webhooks.md § Common headers.
    assert req.headers["content-type"] == "application/json"
    assert req.headers["x-eveys-event-id"] == "evt-int-001"
    assert req.headers["x-eveys-event-type"] == "cp.boot"
    assert req.headers["x-eveys-attempt"] == "1"
    assert "x-eveys-delivered-at" in req.headers

    # Signature verifies against the body bytes the receiver got.
    assert verify_signature(
        bytes(req.content),
        req.headers["x-eveys-signature"],
        s.webhook_secret,
    )

    # Body is the JSON envelope, not the raw protobuf.
    body = json.loads(req.content)
    assert body["success"] is True
    assert body["data"]["cp_id"] == "CP_INT_001"
    assert body["data"]["registration_status"] == "Accepted"

    await d._http.aclose()


@pytest.mark.asyncio
async def test_envelope_for_disabled_event_makes_no_request() -> None:
    captured: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        captured.append(request)
        return httpx.Response(200)

    d = WebhookDispatcher(_settings(webhook_enable_cp_boot=False))
    d._http = httpx.AsyncClient(transport=httpx.MockTransport(receiver))

    await d._deliver_one(_FakeRecord(value=_make_boot_envelope().SerializeToString()))

    assert captured == []
    await d._http.aclose()


@pytest.mark.asyncio
async def test_unparseable_record_is_logged_and_skipped(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A malformed Kafka record (not a valid protobuf) shouldn't crash
    the dispatcher — log + skip."""
    captured: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        captured.append(request)
        return httpx.Response(200)

    d = WebhookDispatcher(_settings())
    d._http = httpx.AsyncClient(transport=httpx.MockTransport(receiver))

    await d._deliver_one(_FakeRecord(value=b"not-a-protobuf"))

    assert captured == []
    await d._http.aclose()


@pytest.mark.asyncio
async def test_cp_online_event_routes_to_online_url() -> None:
    """`cp_connected` proto variant maps to `cp.online` webhook
    (E3-9 follow-up). No producer emits it yet, but the dispatcher
    recognises it for when one shows up."""
    captured: list[httpx.Request] = []

    def receiver(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200)

    d = WebhookDispatcher(_settings())
    d._http = httpx.AsyncClient(transport=httpx.MockTransport(receiver))

    env = events_pb2.EventEnvelope()
    env.event_id = "evt-online-001"
    env.occurred_at = "2026-05-07T14:00:00+00:00"
    env.cp_id = "CP_INT_001"
    env.cp_connected.subprotocol = "ocpp1.6"
    env.cp_connected.pod_id = "pod-7"

    await d._deliver_one(_FakeRecord(value=env.SerializeToString(), topic="cp.connected"))

    assert len(captured) == 1
    req = captured[0]
    assert str(req.url) == "https://backend.example/webhooks/cp-online"
    assert req.headers["x-eveys-event-type"] == "cp.online"

    body = json.loads(req.content)
    assert body["data"]["pod_id"] == "pod-7"

    await d._http.aclose()
