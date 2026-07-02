"""Unit tests for the SignCertificate handler (#186).

Covers:
- Accepted CSR: persists row, replies Accepted, emits cp.csr_submitted.
- Empty CSR: rejected without DB / Kafka traffic.
- No producer (Kafka-less local stack): handler still completes.
- Broker drop: charger still gets Accepted.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result
from ocpp.v16.enums import GenericStatus

from eveys_ocpp.handlers.v16 import sign_certificate

_DUMMY_CSR = (
    "-----BEGIN CERTIFICATE REQUEST-----\n"
    "MIIBjzCB+QIBADAvMSwwKgYDVQQDDCNUZXN0Q1AwMDEuY2hhcmdlcnMuZXZleXMu\n"
    "ZXhhbXBsZS5jb20wgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAMyz9HW3NjL3\n"
    "-----END CERTIFICATE REQUEST-----\n"
)


@pytest.mark.asyncio
async def test_accepted_persists_and_replies_accepted(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    insert = AsyncMock(return_value=42)
    monkeypatch.setattr(sign_certificate, "insert_pending_certificate_signing", insert)

    result = await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR)

    assert isinstance(result, call_result.SignCertificate)
    assert result.status == GenericStatus.accepted
    insert.assert_awaited_once()
    assert insert.await_args is not None
    assert insert.await_args.kwargs["cp_id"] == "TEST_CP_001"
    assert insert.await_args.kwargs["csr"] == _DUMMY_CSR


@pytest.mark.asyncio
async def test_empty_csr_rejected_without_db_call(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty / whitespace-only CSR is malformed; reject upfront with no
    DB write and no Kafka publish."""
    insert = AsyncMock()
    monkeypatch.setattr(sign_certificate, "insert_pending_certificate_signing", insert)
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    for empty in ("", "   ", "\n\t"):
        result = await sign_certificate.handle(fake_cp, csr=empty)
        assert isinstance(result, call_result.SignCertificate)
        assert result.status == GenericStatus.rejected

    insert.assert_not_awaited()
    fake_producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_publishes_envelope_on_accept(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Accepted CSR publishes one CpCsrSubmitted envelope on the
    configured topic, keyed by cp_id, with both csr and pending_id."""
    from eveys_ocpp._generated.events.v1 import events_pb2

    monkeypatch.setattr(
        sign_certificate, "insert_pending_certificate_signing", AsyncMock(return_value=99)
    )
    fake_producer = AsyncMock()
    fake_cp.event_producer = fake_producer

    await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR)

    fake_producer.publish.assert_awaited_once()
    kwargs = fake_producer.publish.await_args.kwargs
    assert kwargs["topic"] == fake_cp.settings.kafka_topic_cp_csr_submitted
    assert kwargs["key"] == "TEST_CP_001"

    envelope = events_pb2.EventEnvelope()
    envelope.ParseFromString(kwargs["value"])
    assert envelope.cp_id == "TEST_CP_001"
    assert envelope.WhichOneof("payload") == "cp_csr_submitted"
    assert envelope.cp_csr_submitted.csr == _DUMMY_CSR
    assert envelope.cp_csr_submitted.pending_id == 99


@pytest.mark.asyncio
async def test_no_producer_does_not_publish(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """Kafka-less local stack: handler completes cleanly, no crash."""
    monkeypatch.setattr(
        sign_certificate, "insert_pending_certificate_signing", AsyncMock(return_value=1)
    )
    fake_cp.event_producer = None
    result = await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR)
    assert result.status == GenericStatus.accepted


@pytest.mark.asyncio
async def test_publish_failure_does_not_crash_handler(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Best-effort publish: a broker drop must not break the OCPP
    response — the charger still gets Accepted."""
    monkeypatch.setattr(
        sign_certificate, "insert_pending_certificate_signing", AsyncMock(return_value=7)
    )
    fake_producer = AsyncMock()
    fake_producer.publish = AsyncMock(side_effect=RuntimeError("kafka down"))
    fake_cp.event_producer = fake_producer

    result = await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR)
    assert isinstance(result, call_result.SignCertificate)
    assert result.status == GenericStatus.accepted


@pytest.mark.asyncio
async def test_ignores_extra_kwargs(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sign_certificate, "insert_pending_certificate_signing", AsyncMock(return_value=1)
    )
    result = await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR, request_id=123)
    assert isinstance(result, call_result.SignCertificate)


@pytest.mark.asyncio
async def test_pending_cp_raises_security_error(fake_cp: Any) -> None:
    """A pending device must be refused with a CALLERROR and never
    touch Postgres."""
    from unittest.mock import MagicMock

    from ocpp.exceptions import SecurityError

    fake_cp.is_pending = True
    fake_cp.session_factory = MagicMock(
        side_effect=AssertionError("session_factory must not be used while pending")
    )

    with pytest.raises(SecurityError):
        await sign_certificate.handle(fake_cp, csr=_DUMMY_CSR)
