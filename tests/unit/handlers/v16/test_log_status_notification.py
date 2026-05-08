"""Unit tests for the LogStatusNotification handler (TC_079).

OCPP 1.6 Security Whitepaper §4.6. Charger reports upload progress
on a previously-issued GetLog. Mirrors the
firmware/diagnostics_status_notification pattern: latest-wins on
`charge_points.last_log_status`, empty conf reply.

Verify-fails dance documented in the PR: the handler MUST call
`update_log_status` (otherwise the operator dashboard never shows
the upload finishing), and the metric MUST carry the status label
(otherwise the SIEM panel goes silent).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest
from ocpp.v16 import call_result

from eveys_ocpp.handlers.v16 import log_status_notification


@pytest.mark.asyncio
async def test_records_status_and_returns_empty_conf(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: handler invokes `update_log_status` with the
    charger-reported status and returns the empty conf the spec
    mandates."""
    update = AsyncMock()
    monkeypatch.setattr(log_status_notification, "update_log_status", update)

    result = await log_status_notification.handle(fake_cp, status="Uploaded", request_id=42)

    assert isinstance(result, call_result.LogStatusNotification)
    update.assert_awaited_once()
    kwargs = update.await_args.kwargs
    assert kwargs == {"cp_id": "TEST_CP_001", "status": "Uploaded"}


@pytest.mark.asyncio
async def test_persists_failure_status(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """`UploadFailure` is the operator-paging value — must round-
    trip identically. The whole point of the dashboard is operator
    visibility into security-log upload failures."""
    update = AsyncMock()
    monkeypatch.setattr(log_status_notification, "update_log_status", update)

    await log_status_notification.handle(fake_cp, status="UploadFailure", request_id=99)

    assert update.await_args.kwargs["status"] == "UploadFailure"


@pytest.mark.asyncio
async def test_handler_does_not_require_request_id(
    fake_cp: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OCPP marks `request_id` required, but charger firmwares vary;
    a missing field shouldn't crash the handler. The status itself
    is what we persist."""
    update = AsyncMock()
    monkeypatch.setattr(log_status_notification, "update_log_status", update)

    result = await log_status_notification.handle(fake_cp, status="Uploading")

    assert isinstance(result, call_result.LogStatusNotification)
    update.assert_awaited_once()


@pytest.mark.asyncio
async def test_metric_label_carries_status(fake_cp: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    """`LOG_STATUS_TOTAL{status}` is what operator alerting reads to
    page on `UploadFailure`. Without the label, the dashboard panel
    silently empties."""
    from eveys_ocpp.metrics import registry as metrics_registry

    monkeypatch.setattr(log_status_notification, "update_log_status", AsyncMock())

    before = metrics_registry.LOG_STATUS_TOTAL.labels(status="Uploaded")._value.get()

    await log_status_notification.handle(fake_cp, status="Uploaded", request_id=42)

    after = metrics_registry.LOG_STATUS_TOTAL.labels(status="Uploaded")._value.get()
    assert after == before + 1
