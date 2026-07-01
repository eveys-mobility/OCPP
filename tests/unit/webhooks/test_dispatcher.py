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
    elif payload_kind == "cp_connected":
        c = env.cp_connected
        c.subprotocol = fields.pop("subprotocol", "ocpp1.6")
        c.pod_id = fields.pop("pod_id", "ocpp-gw-7b3fc9d-x4z8q")
    elif payload_kind == "cp_disconnected":
        d = env.cp_disconnected
        d.pod_id = fields.pop("pod_id", "ocpp-gw-7b3fc9d-x4z8q")
        d.reason = fields.pop("reason", "clean")
    elif payload_kind == "tx_stopped":
        ts = env.tx_stopped
        ts.transaction_id = fields.pop("transaction_id", 12345)
        ts.id_tag = fields.pop("id_tag", "RFID_X")
        ts.meter_stop_wh = fields.pop("meter_stop_wh", 1_023_500)
        ts.consumed_wh = fields.pop("consumed_wh", 23_500)
        ts.stop_reason = fields.pop("stop_reason", "Local")
        ts.charger_reported_at = fields.pop("charger_reported_at", "2026-05-07T12:30:00+00:00")
    elif payload_kind == "cp_firmware_status_changed":
        env.cp_firmware_status_changed.status = fields.pop("status", "Downloading")
    elif payload_kind == "cp_diagnostics_status_changed":
        env.cp_diagnostics_status_changed.status = fields.pop("status", "Uploading")
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


def test_build_body_tx_stopped_shape() -> None:
    """tx.stopped envelope mirrors the documented contract shape per
    `docs/integration/03-webhooks.md`."""
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("tx_stopped"))
    assert body is not None
    data = body["data"]
    assert data["event_type"] == "tx.stopped"
    assert data["transaction_id"] == 12345
    assert data["id_tag"] == "RFID_X"
    assert data["meter_stop_wh"] == 1_023_500
    assert data["consumed_wh"] == 23_500
    assert data["stop_reason"] == "Local"
    assert data["charger_reported_at"] == "2026-05-07T12:30:00+00:00"


def test_build_body_tx_stopped_returns_none_when_disabled() -> None:
    d = WebhookDispatcher(_settings(webhook_enable_tx_stopped=False))
    assert d._build_body(_envelope("tx_stopped")) is None


def test_build_body_cp_offline_shape() -> None:
    """cp.offline envelope mirrors the documented contract shape per
    `docs/integration/03-webhooks.md` § cp.online / cp.offline."""
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("cp_disconnected", reason="error"))
    assert body is not None
    data = body["data"]
    assert data["event_type"] == "cp.offline"
    assert data["cp_id"] == "CP_TEST"
    assert data["pod_id"] == "ocpp-gw-7b3fc9d-x4z8q"
    assert data["reason"] == "error"


def test_build_body_cp_offline_returns_none_when_disabled() -> None:
    d = WebhookDispatcher(_settings(webhook_enable_cp_offline=False))
    assert d._build_body(_envelope("cp_disconnected")) is None


def test_build_body_cp_firmware_status_changed_shape() -> None:
    """cp.firmware_status_changed envelope mirrors the documented
    contract shape per `docs/integration/03-webhooks.md`."""
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("cp_firmware_status_changed", status="Downloading"))
    assert body is not None
    data = body["data"]
    assert data["event_type"] == "cp.firmware_status_changed"
    assert data["cp_id"] == "CP_TEST"
    assert data["status"] == "Downloading"


def test_build_body_cp_firmware_status_changed_returns_none_when_disabled() -> None:
    d = WebhookDispatcher(_settings(webhook_enable_cp_firmware_status=False))
    assert d._build_body(_envelope("cp_firmware_status_changed")) is None


def test_build_body_cp_diagnostics_status_changed_shape() -> None:
    d = WebhookDispatcher(_settings())
    body = d._build_body(_envelope("cp_diagnostics_status_changed", status="Uploading"))
    assert body is not None
    data = body["data"]
    assert data["event_type"] == "cp.diagnostics_status_changed"
    assert data["cp_id"] == "CP_TEST"
    assert data["status"] == "Uploading"


def test_build_body_cp_diagnostics_status_changed_returns_none_when_disabled() -> None:
    d = WebhookDispatcher(_settings(webhook_enable_cp_diagnostics_status=False))
    assert d._build_body(_envelope("cp_diagnostics_status_changed")) is None


def test_build_body_returns_none_for_disabled_event() -> None:
    """`webhook_enable_cp_boot=False` → cp.boot skipped silently."""
    d = WebhookDispatcher(_settings(webhook_enable_cp_boot=False))
    assert d._build_body(_envelope("cp_boot")) is None


def test_build_body_returns_none_when_cp_meter_disabled() -> None:
    """`webhook_enable_cp_meter=False` → cp.meter skipped silently."""
    d = WebhookDispatcher(_settings(webhook_enable_cp_meter=False))
    env = events_pb2.EventEnvelope()
    env.event_id = "e"
    env.cp_id = "CP_TEST"
    env.cp_meter.connector_id = 1
    assert d._build_body(env) is None


def test_build_body_emits_cp_meter_with_sampled_values_by_default() -> None:
    """Default-on: cp.meter envelope → OCPP-wire JSON body, enum→string."""
    d = WebhookDispatcher(_settings())
    env = events_pb2.EventEnvelope()
    env.event_id = "e"
    env.occurred_at = "2026-05-07T12:00:00+00:00"
    env.cp_id = "CP_TEST"
    m = env.cp_meter
    m.connector_id = 1
    m.transaction_id = 42
    m.charger_reported_at = "2026-05-07T12:00:00+00:00"
    sv = m.sampled_values.add()
    sv.value = "1234.5"
    sv.measurand = events_pb2.MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER
    sv.unit = events_pb2.UNIT_WH
    sv.context = events_pb2.CONTEXT_SAMPLE_PERIODIC
    body = d._build_body(env)
    assert body is not None
    data = body["data"]
    assert data["event_type"] == "cp.meter_values"
    assert data["connector_id"] == 1
    assert data["transaction_id"] == 42
    [s] = data["sampled_values"]
    assert s["value"] == "1234.5"
    assert s["measurand"] == "Energy.Active.Import.Register"
    assert s["unit"] == "Wh"
    assert s["context"] == "Sample.Periodic"


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


def test_url_for_tx_stopped_falls_back_to_base() -> None:
    d = WebhookDispatcher(_settings())
    assert d._url_for(_envelope("tx_stopped")) == "https://backend.example/webhooks/tx-stopped"


def test_url_for_tx_stopped_uses_override() -> None:
    d = WebhookDispatcher(_settings(webhook_url_tx_stopped="https://override.example/closure"))
    assert d._url_for(_envelope("tx_stopped")) == "https://override.example/closure"


def test_url_for_cp_offline_falls_back_to_base() -> None:
    d = WebhookDispatcher(_settings())
    assert d._url_for(_envelope("cp_disconnected")) == "https://backend.example/webhooks/cp-offline"


def test_url_for_cp_offline_uses_override() -> None:
    d = WebhookDispatcher(
        _settings(webhook_url_cp_offline="https://override.example/presence/down")
    )
    assert d._url_for(_envelope("cp_disconnected")) == "https://override.example/presence/down"


def test_url_for_cp_firmware_status_falls_back_to_base() -> None:
    d = WebhookDispatcher(_settings())
    assert d._url_for(_envelope("cp_firmware_status_changed")) == (
        "https://backend.example/webhooks/cp-firmware-status-changed"
    )


def test_url_for_cp_firmware_status_uses_override() -> None:
    d = WebhookDispatcher(_settings(webhook_url_cp_firmware_status="https://override.example/fw"))
    assert d._url_for(_envelope("cp_firmware_status_changed")) == "https://override.example/fw"


def test_url_for_cp_diagnostics_status_falls_back_to_base() -> None:
    d = WebhookDispatcher(_settings())
    assert d._url_for(_envelope("cp_diagnostics_status_changed")) == (
        "https://backend.example/webhooks/cp-diagnostics-status-changed"
    )


def test_url_for_cp_diagnostics_status_uses_override() -> None:
    d = WebhookDispatcher(
        _settings(webhook_url_cp_diagnostics_status="https://override.example/diag")
    )
    assert d._url_for(_envelope("cp_diagnostics_status_changed")) == "https://override.example/diag"


# ---- _enabled_topics -------------------------------------------------------


def test_enabled_topics_default() -> None:
    d = WebhookDispatcher(_settings())
    topics = d._enabled_topics()
    # Default: every event is enabled, including cp.meter — fresh
    # installs get per-frame consumption rows without extra wiring.
    # Large fleets (>100 chargers) should set
    # `webhook_enable_cp_meter=False` and consume the `cp.meter` Kafka
    # topic directly.
    assert "cp.boot" in topics
    assert "cp.connected" in topics
    assert "cp.disconnected" in topics
    assert "cp.status" in topics
    assert "cp.firmware_status" in topics
    assert "cp.diagnostics_status" in topics
    assert "tx.started" in topics
    assert "tx.stopped" in topics
    assert "cp.meter" in topics


def test_enabled_topics_all_off() -> None:
    d = WebhookDispatcher(
        _settings(
            webhook_enable_cp_boot=False,
            webhook_enable_cp_online=False,
            webhook_enable_cp_offline=False,
            webhook_enable_cp_heartbeat=False,
            webhook_enable_cp_status=False,
            webhook_enable_tx_started=False,
            webhook_enable_tx_stopped=False,
            webhook_enable_cp_firmware_status=False,
            webhook_enable_cp_diagnostics_status=False,
            webhook_enable_cp_meter=False,
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
async def test_post_retries_on_4xx() -> None:
    """Under the "only 2xx = accepted" contract, 4xx codes are
    retried just like 5xx. A backend that 401s during token rotation
    or 400s during a rolling schema deploy still gets retried; the
    envelope only lands in the backlog after the in-loop budget."""
    d = WebhookDispatcher(_settings(webhook_max_attempts=3))
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

    # All three attempts fired against the 4xx — no early-exit path.
    assert d._http.post.await_count == 3


@pytest.mark.asyncio
async def test_post_retries_on_429_alongside_other_4xx() -> None:
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


# ---- outbound HTTP request shape (issue #102 — audit Finding 2) -----------
#
# Existing tests above mock `d._http.post` and check the retry counter,
# never the request shape itself. A typo in a header name (e.g. an
# accidental rename to `X-Eveys-Sig`) would still pass every test, but
# the backend would reject every webhook with 400 and delivery would
# silently die. These tests pin the contract documented in
# `docs/integration/03-webhooks.md` § "Headers".


@pytest.mark.asyncio
async def test_post_sends_documented_headers_and_body() -> None:
    """The five `X-Eveys-*` headers + Content-Type must match the
    contract exactly. Captures the kwargs `_http.post` is actually
    called with — a missing or renamed header fails this test
    instantly."""
    d = WebhookDispatcher(_settings(webhook_max_attempts=1))
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(200))

    await d._post_with_retry(
        url="https://backend.example/webhooks/cp-boot",
        body_bytes=b'{"hello":"world"}',
        signature="sha256=abc123",
        event_id="evt-headers-1",
        event_type="cp.boot",
    )

    # Inspect the actual outbound request — not just "was called".
    call = d._http.post.await_args
    assert call.args == ("https://backend.example/webhooks/cp-boot",)
    assert call.kwargs["content"] == b'{"hello":"world"}'
    headers = call.kwargs["headers"]

    # Header NAMES are part of the contract. A typo here would silently
    # break delivery.
    assert headers["Content-Type"] == "application/json"
    assert headers["X-Eveys-Signature"] == "sha256=abc123"
    assert headers["X-Eveys-Event-Id"] == "evt-headers-1"
    assert headers["X-Eveys-Event-Type"] == "cp.boot"
    assert headers["X-Eveys-Attempt"] == "1"
    # X-Eveys-Delivered-At is dynamic but must be present + ISO-8601.
    delivered_at = headers["X-Eveys-Delivered-At"]
    from datetime import datetime

    parsed = datetime.fromisoformat(delivered_at)  # raises if not ISO-8601
    assert parsed.tzinfo is not None, "X-Eveys-Delivered-At must be tz-aware UTC"

    # No stray extra headers — the contract is a closed set.
    expected = {
        "Content-Type",
        "X-Eveys-Signature",
        "X-Eveys-Event-Id",
        "X-Eveys-Event-Type",
        "X-Eveys-Delivered-At",
        "X-Eveys-Attempt",
    }
    assert set(headers.keys()) == expected


@pytest.mark.asyncio
async def test_post_attempt_header_increments_on_retry() -> None:
    """`X-Eveys-Attempt` must reflect the current retry attempt
    (1, 2, 3...) — not stay at 1 forever. The backend uses this
    for at-least-once dedup decisions."""
    d = WebhookDispatcher(_settings(webhook_max_attempts=3))
    d._http = MagicMock()
    d._http.post = AsyncMock(side_effect=[_resp(503), _resp(503), _resp(200)])

    with patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="evt-attempt",
            event_type="cp.boot",
        )

    attempts = [c.kwargs["headers"]["X-Eveys-Attempt"] for c in d._http.post.await_args_list]
    assert attempts == ["1", "2", "3"]


# ---- signature round-trip (issue #102 — audit Finding 6) ------------------
#
# The signer's unit tests (`test_signer.py`) verify compute_signature ↔
# verify_signature with hardcoded inputs. What's missing: that the bytes
# the dispatcher actually puts on the wire are the bytes it signed.
# A future change to body serialization (added whitespace, different
# encoding, late JSON mutation) would silently break delivery — the
# receiver's verify_signature would reject every webhook.


async def _drive_one_delivery(
    d: WebhookDispatcher,
    envelope: events_pb2.EventEnvelope,
) -> tuple[bytes, str]:
    """Drive the dispatcher's serialize → sign → POST chain for one
    envelope and return (body_on_wire, signature_header).

    `_deliver_one` is what the Kafka loop calls per record, but its
    signature takes a `ConsumerRecord`. We synthesize one with the
    serialized envelope as `value` so the same code path runs as in
    production — no shortcut to internal helpers that could drift
    from the real flow."""
    from aiokafka.structs import ConsumerRecord

    record = ConsumerRecord(
        topic="cp.boot.v1",
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=envelope.SerializeToString(),
        checksum=None,
        serialized_key_size=0,
        serialized_value_size=0,
        headers=[],
    )
    await d._deliver_one(record)
    call = d._http.post.await_args
    return call.kwargs["content"], call.kwargs["headers"]["X-Eveys-Signature"]


@pytest.mark.asyncio
async def test_dispatcher_body_round_trips_through_verify_signature() -> None:
    """The body bytes the dispatcher sends on the wire must verify
    against the signature it sends in the X-Eveys-Signature header.
    Catches any future divergence between what's signed and what's
    sent (whitespace, encoding, late mutation, etc.)."""
    from eveys_ocpp.webhooks.signer import verify_signature

    secret = "shared-secret-for-roundtrip"
    d = WebhookDispatcher(
        _settings(
            webhook_secret=secret,
            webhook_max_attempts=1,
            # Per-event URL must be present for the dispatcher to deliver.
            webhook_url_cp_boot="https://backend.example/webhooks/cp-boot",
        )
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(200))

    body_on_wire, sig_header = await _drive_one_delivery(
        d, _envelope("cp_boot", event_id="evt-roundtrip-1", cp_id="CP_ROUNDTRIP")
    )

    # The receiver's verify must accept these exact bytes + header
    # under the same secret. If the dispatcher signed-then-mutated,
    # this would return False.
    assert verify_signature(body_on_wire, sig_header, secret) is True

    # And rejects under a wrong secret (sanity check that the
    # round-trip isn't trivially-true).
    assert verify_signature(body_on_wire, sig_header, "wrong-secret") is False


@pytest.mark.asyncio
async def test_dispatcher_signature_changes_when_body_changes() -> None:
    """Two different envelopes produce two different signatures.
    Catches a hypothetical bug where the dispatcher accidentally
    signs a constant string instead of the body."""
    from eveys_ocpp.webhooks.signer import verify_signature

    secret = "shared-secret"
    d = WebhookDispatcher(
        _settings(
            webhook_secret=secret,
            webhook_max_attempts=1,
            webhook_url_cp_boot="https://backend.example/webhooks/cp-boot",
        )
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(200))

    body_a, sig_a = await _drive_one_delivery(
        d, _envelope("cp_boot", event_id="evt-A", cp_id="CP_A", vendor="VendorA")
    )
    body_b, sig_b = await _drive_one_delivery(
        d, _envelope("cp_boot", event_id="evt-B", cp_id="CP_B", vendor="VendorB")
    )

    assert body_a != body_b
    assert sig_a != sig_b
    # Each signature only verifies its own body — never the other's.
    assert verify_signature(body_a, sig_a, secret) is True
    assert verify_signature(body_b, sig_b, secret) is True
    assert verify_signature(body_a, sig_b, secret) is False
    assert verify_signature(body_b, sig_a, secret) is False


# ---- backlog enqueue on exhaustion ----------------------------------------


def _make_session_factory() -> tuple[MagicMock, Any]:
    """Return a ``(session_mock, session_factory)`` pair the dispatcher
    accepts as its Postgres dependency. The factory context yields the
    session so ``async with session_factory() as s:`` works."""
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()

    class _Ctx:
        async def __aenter__(self) -> MagicMock:
            return session

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _Factory:
        def __call__(self) -> _Ctx:
            return _Ctx()

    return session, _Factory()


@pytest.mark.asyncio
async def test_post_enqueues_backlog_after_exhausted_retries() -> None:
    """Backend 5xx-ing through every attempt: the dispatcher gives up
    in-loop AND inserts one row into the backlog, both."""
    session, factory = _make_session_factory()
    d = WebhookDispatcher(
        _settings(webhook_max_attempts=2, webhook_backlog_enabled=True),
        session_factory=factory,
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(503))

    valid_uuid = "00000000-0000-0000-0000-000000000001"
    with (
        patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()),
        patch("eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog", AsyncMock()) as mock_insert,
    ):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id=valid_uuid,
            event_type="cp.boot",
        )

    assert d._http.post.await_count == 2
    mock_insert.assert_awaited_once()
    call = mock_insert.await_args
    assert str(call.kwargs["event_id"]) == valid_uuid
    assert call.kwargs["event_type"] == "cp.boot"
    assert call.kwargs["url"] == "https://x/test"
    assert call.kwargs["body"] == b"{}"
    assert call.kwargs["signature"] == "sha256=abc"
    session.commit.assert_awaited_once()


@pytest.mark.asyncio
async def test_post_skips_backlog_when_disabled() -> None:
    """`webhook_backlog_enabled=False` restores the old drop-on-exhaust
    behaviour — no insert attempt even with a factory wired."""
    _session, factory = _make_session_factory()
    d = WebhookDispatcher(
        _settings(webhook_max_attempts=1, webhook_backlog_enabled=False),
        session_factory=factory,
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(503))

    with (
        patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()),
        patch("eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog", AsyncMock()) as mock_insert,
    ):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="00000000-0000-0000-0000-000000000002",
            event_type="cp.boot",
        )

    mock_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_skips_backlog_when_no_session_factory() -> None:
    """Dispatcher constructed without a session_factory (legacy
    callers, tests that don't touch Postgres): flag is on but there's
    no place to write to. Should log + skip, never crash."""
    d = WebhookDispatcher(_settings(webhook_max_attempts=1, webhook_backlog_enabled=True))
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(503))

    with (
        patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()),
        patch("eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog", AsyncMock()) as mock_insert,
    ):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="00000000-0000-0000-0000-000000000003",
            event_type="cp.boot",
        )

    mock_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_does_not_enqueue_backlog_on_success() -> None:
    """Delivered on first try — nothing to persist for the drainer."""
    _session, factory = _make_session_factory()
    d = WebhookDispatcher(
        _settings(webhook_max_attempts=3, webhook_backlog_enabled=True),
        session_factory=factory,
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(200))

    with patch("eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog", AsyncMock()) as mock_insert:
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="00000000-0000-0000-0000-000000000004",
            event_type="cp.boot",
        )

    mock_insert.assert_not_awaited()


@pytest.mark.asyncio
async def test_post_enqueues_backlog_after_4xx_exhausts() -> None:
    """Under the "only 2xx = accepted" contract, 4xx is retryable —
    so the dispatcher walks its full budget against a persistent 400
    and then enqueues into the backlog. Old semantics (4xx = permanent
    reject, no backlog) is intentionally NOT preserved: the customer
    would rather retry a bad-json 400 for 7 days than lose it."""
    _session, factory = _make_session_factory()
    d = WebhookDispatcher(
        _settings(webhook_max_attempts=2, webhook_backlog_enabled=True),
        session_factory=factory,
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(400))

    with (
        patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()),
        patch("eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog", AsyncMock()) as mock_insert,
    ):
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="00000000-0000-0000-0000-000000000005",
            event_type="cp.boot",
        )

    # Both attempts fired against 400, THEN the backlog row landed.
    assert d._http.post.await_count == 2
    mock_insert.assert_awaited_once()


@pytest.mark.asyncio
async def test_post_swallows_backlog_insert_errors() -> None:
    """Postgres down during enqueue mustn't kill the delivery loop —
    the envelope is already lost but the dispatcher keeps consuming."""
    _session, factory = _make_session_factory()
    d = WebhookDispatcher(
        _settings(webhook_max_attempts=1, webhook_backlog_enabled=True),
        session_factory=factory,
    )
    d._http = MagicMock()
    d._http.post = AsyncMock(return_value=_resp(503))

    with (
        patch("eveys_ocpp.webhooks.dispatcher.asyncio.sleep", AsyncMock()),
        patch(
            "eveys_ocpp.webhooks.dispatcher.insert_webhook_backlog",
            AsyncMock(side_effect=RuntimeError("db down")),
        ) as mock_insert,
    ):
        # Should not raise even though the insert did.
        await d._post_with_retry(
            url="https://x/test",
            body_bytes=b"{}",
            signature="sha256=abc",
            event_id="00000000-0000-0000-0000-000000000006",
            event_type="cp.boot",
        )

    mock_insert.assert_awaited_once()
