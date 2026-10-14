"""Archive availability is separate from a current boot-epoch status observation."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp.api.charge_points import _enrich_with_connectors


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status_at,boot_at,freshness,reason",
    [
        (
            datetime(2026, 10, 13, 8, 47, 4, tzinfo=UTC),
            datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC),
            "stale",
            "before_boot",
        ),
        (
            datetime(2026, 10, 13, 9, 30, tzinfo=UTC),
            datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC),
            "current",
            None,
        ),
        (datetime(2020, 1, 2, tzinfo=UTC), datetime(2020, 1, 1, tzinfo=UTC), "current", None),
        (None, datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC), "unknown", "unverified_timestamp"),
        (datetime(2026, 10, 13, 9, 30, tzinfo=UTC), None, "unknown", "unverified_timestamp"),
        (
            datetime(2026, 10, 13, 11, 47, 4, tzinfo=timezone(timedelta(hours=3))),
            datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC),
            "stale",
            "before_boot",
        ),
        (
            datetime(2026, 10, 13, 8, 47, 4),
            datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC),
            "stale",
            "before_boot",
        ),
        (
            datetime(2026, 10, 13, 9, 12, 44, 123000, tzinfo=UTC),
            datetime(2026, 10, 13, 9, 12, 44, 123456, tzinfo=UTC),
            "current",
            None,
        ),
        (None, None, "unknown", "unverified_timestamp"),
        (
            "invalid-server-time",
            datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC),
            "unknown",
            "unverified_timestamp",
        ),
        (
            datetime(2026, 10, 13, 9, 30, tzinfo=UTC),
            "invalid-boot-time",
            "unknown",
            "unverified_timestamp",
        ),
    ],
)
async def test_connector_uses_server_observation_and_boot_epoch_without_age_ttl(
    status_at, boot_at, freshness, reason
):
    # asynch may decode the declared UTC DateTime64 as naive UTC.
    client = SimpleNamespace(
        fetch_latest_connector_statuses=AsyncMock(
            return_value={
                "CP_TEST": [
                    {
                        "connector_id": 1,
                        "status": "Charging",
                        "error_code": "NoError",
                        "last_changed_at": status_at,
                    }
                ]
            }
        )
    )
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ch_client=client)))
    result = await _enrich_with_connectors(request, [{"cp_id": "CP_TEST", "last_boot_at": boot_at}])
    (cp,) = result
    (connector,) = cp["connectors"]
    assert connector["status_freshness"] == freshness
    assert connector["stale_reason"] == reason
    assert connector["status"] == "Charging"
    assert connector["error_code"] == "NoError"
    assert cp["connectors_state"] == "available"
    assert cp["connectors_last_successful_at"] is not None
    if isinstance(status_at, datetime):
        parsed = datetime.fromisoformat(connector["last_changed_at"])
        expected = (
            status_at.replace(tzinfo=UTC) if status_at.tzinfo is None else status_at.astimezone(UTC)
        )
        assert parsed.utcoffset() == timedelta(0)
        assert parsed == expected
    else:
        assert connector["last_changed_at"] is None


def test_schema_carries_optional_additive_freshness_and_server_observation_time():
    from eveys_ocpp.api._schemas import ConnectorStatus

    row = ConnectorStatus(
        connector_id=1,
        status="Charging",
        status_freshness="stale",
        stale_reason="before_boot",
        last_changed_at="2026-10-13T08:47:04+00:00",
    )
    assert row.model_dump()["status_freshness"] == "stale"
    assert row.model_dump()["stale_reason"] == "before_boot"
    assert row.model_dump()["last_changed_at"] == "2026-10-13T08:47:04+00:00"
    assert ConnectorStatus(connector_id=1, status="Charging").status_freshness is None


@pytest.mark.asyncio
@pytest.mark.parametrize("detail", [False, True])
@pytest.mark.parametrize(
    "observed",
    [
        datetime(2026, 10, 13, 8, 47, 4, tzinfo=UTC),
        datetime(2026, 10, 13, 8, 47, 4),
        datetime(2026, 10, 13, 11, 47, 4, tzinfo=timezone(timedelta(hours=3))),
    ],
)
async def test_list_and_detail_preserve_raw_charging_but_report_stale_after_boot(
    client, monkeypatch, fake_ch_client, detail, observed
):
    from eveys_ocpp.api import charge_points as module
    from tests.unit.api.test_charge_points import _cp_row

    cp = _cp_row()
    cp["last_boot_at"] = datetime(2026, 10, 13, 9, 12, 44, tzinfo=UTC)
    cp["active_reservations"] = []
    cp["active_charging_profiles"] = []
    monkeypatch.setattr(module, "list_charge_points", AsyncMock(return_value=[cp]))
    monkeypatch.setattr(module, "get_charge_point_detail", AsyncMock(return_value=cp))
    monkeypatch.setattr(module, "list_transactions_by_cp", AsyncMock(return_value=[]))
    fake_ch_client.fetch_latest_connector_statuses = AsyncMock(
        return_value={
            "CP_001": [
                {
                    "connector_id": 1,
                    "status": "Charging",
                    "error_code": "NoError",
                    "last_changed_at": observed,
                }
            ]
        }
    )
    response = await client.get(
        "/api/v1/charge-points/CP_001" if detail else "/api/v1/charge-points"
    )
    assert response.status_code == 200
    body = response.json() if detail else response.json()["charge_points"][0]
    (connector,) = body["connectors"]
    assert connector == {
        "connector_id": 1,
        "status": "Charging",
        "error_code": "NoError",
        "last_changed_at": "2026-10-13T08:47:04+00:00",
        "status_freshness": "stale",
        "stale_reason": "before_boot",
    }
    assert body["connectors_state"] == "available"
    assert body["connectors_last_successful_at"] != connector["last_changed_at"]
