"""Tests for the gateway command endpoints (E3-8).

Covers all 19 commands: 18 POST + 1 GET. Each route is exercised for
the happy path; representative routes also cover the four error-mapping
cases (UNKNOWN_CP_ID, CHARGER_OFFLINE, CHARGER_TIMEOUT, INTERNAL_ERROR)
since the mapping is centralised in `_commands.dispatch_ocpp_call`.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from grpclib.const import Status
from grpclib.exceptions import GRPCError

# ---- helpers ---------------------------------------------------------------


def _stub_response(**fields: Any) -> SimpleNamespace:
    """Build a fake OCPP response with `status` plus any extras."""
    return SimpleNamespace(**fields)


def _set_response(fake_command_service: MagicMock, response: SimpleNamespace | Exception) -> None:
    if isinstance(response, Exception):
        fake_command_service._dispatch_ocpp_call.side_effect = response
    else:
        fake_command_service._dispatch_ocpp_call.return_value = response
        fake_command_service._dispatch_ocpp_call.side_effect = None


# ---- core remote control ---------------------------------------------------


@pytest.mark.asyncio
async def test_remote_start_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-start",
        json={"id_tag": "RFID_X", "connector_id": 1},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "Accepted"
    assert body["request_id"]
    # Spy: the dispatcher saw the right OCPP dataclass.
    call = fake_command_service._dispatch_ocpp_call.await_args
    assert call.kwargs["rpc"] == "RemoteStart"
    assert call.kwargs["cp_id"] == "CP_001"
    assert call.kwargs["ocpp_request"].id_tag == "RFID_X"
    assert call.kwargs["ocpp_request"].connector_id == 1


@pytest.mark.asyncio
async def test_remote_start_missing_id_tag_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-start",
        json={"connector_id": 1},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


@pytest.mark.asyncio
async def test_remote_stop_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-stop",
        json={"transaction_id": 12345},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "Accepted"
    assert (
        fake_command_service._dispatch_ocpp_call.await_args.kwargs["ocpp_request"].transaction_id
        == 12345
    )


@pytest.mark.asyncio
async def test_remote_stop_rejects_string_transaction_id(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-stop",
        json={"transaction_id": "not-an-int"},
    )
    assert response.status_code == 400
    assert "transaction_id" in response.json()["error"]


@pytest.mark.asyncio
async def test_reset_happy_path(client: httpx.AsyncClient, fake_command_service: MagicMock) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/reset",
        json={"type": "Soft"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "Accepted"


@pytest.mark.asyncio
async def test_reset_invalid_type_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/reset",
        json={"type": "Sideways"},
    )
    assert response.status_code == 400


# ---- configuration ---------------------------------------------------------


@pytest.mark.asyncio
async def test_change_configuration_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="RebootRequired"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/change-configuration",
        json={"key": "HeartbeatInterval", "value": "120"},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "RebootRequired"


@pytest.mark.asyncio
async def test_get_configuration_returns_keys_and_unknown(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        _stub_response(
            configuration_key=[{"key": "HeartbeatInterval", "readonly": False, "value": "120"}],
            unknown_key=["MysteryKey"],
        ),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-configuration",
        json={"keys": ["HeartbeatInterval", "MysteryKey"]},
    )

    body = response.json()
    assert response.status_code == 200
    assert body["configuration_key"][0]["key"] == "HeartbeatInterval"
    assert body["unknown_key"] == ["MysteryKey"]


@pytest.mark.asyncio
async def test_get_configuration_empty_body_means_all(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(configuration_key=[], unknown_key=[]))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-configuration",
        json={},
    )
    assert response.status_code == 200
    # Empty list of keys → forwarded as None to the OCPP layer (=> "all").
    assert fake_command_service._dispatch_ocpp_call.await_args.kwargs["ocpp_request"].key is None


@pytest.mark.asyncio
async def test_clear_cache_accepts_empty_body(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post("/api/v1/charge-points/CP_001/commands/clear-cache")
    assert response.status_code == 200
    assert response.json()["status"] == "Accepted"


@pytest.mark.asyncio
async def test_trigger_message_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/trigger-message",
        json={"requested_message": "Heartbeat", "connector_id": 1},
    )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_trigger_message_invalid_kind_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/trigger-message",
        json={"requested_message": "NotARealMessage"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_unlock_connector_requires_positive_id(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/unlock-connector",
        json={"connector_id": 0},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_unlock_connector_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Unlocked"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/unlock-connector",
        json={"connector_id": 1},
    )
    assert response.status_code == 200
    assert response.json()["status"] == "Unlocked"


# ---- vendor extension ------------------------------------------------------


@pytest.mark.asyncio
async def test_data_transfer_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        _stub_response(status="Accepted", data='{"echo":true}'),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/data-transfer",
        json={"vendor_id": "Eveys", "message_id": "ping", "data": "hello"},
    )

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "Accepted"
    assert body["data"] == '{"echo":true}'


@pytest.mark.asyncio
async def test_data_transfer_requires_vendor_id(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/data-transfer",
        json={"message_id": "ping"},
    )
    assert response.status_code == 400


# ---- LocalAuthList ---------------------------------------------------------


@pytest.mark.asyncio
async def test_get_local_list_version_happy_path(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(list_version=11))

    response = await client.post("/api/v1/charge-points/CP_001/commands/get-local-list-version")
    assert response.status_code == 200
    assert response.json()["list_version"] == 11


@pytest.mark.asyncio
async def test_send_local_list_full_persists_on_accepted(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Accepted"))
    persist = AsyncMock()
    monkeypatch.setattr(cmd_module, "replace_local_auth_list", persist)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/send-local-list",
        json={
            "list_version": 12,
            "update_type": "Full",
            "local_authorization_list": [{"id_tag": "TAG1", "id_tag_info": {"status": "Accepted"}}],
        },
    )

    assert response.status_code == 200
    assert persist.await_count == 1
    assert persist.await_args.kwargs["list_version"] == 12


@pytest.mark.asyncio
async def test_send_local_list_differential_persists_via_diff(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Accepted"))
    persist_diff = AsyncMock()
    monkeypatch.setattr(cmd_module, "apply_local_auth_list_differential", persist_diff)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/send-local-list",
        json={
            "list_version": 13,
            "update_type": "Differential",
            "local_authorization_list": [{"id_tag": "TAG2"}],
        },
    )

    assert response.status_code == 200
    assert persist_diff.await_count == 1


@pytest.mark.asyncio
async def test_send_local_list_does_not_persist_on_rejection(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="VersionMismatch"))
    persist = AsyncMock()
    monkeypatch.setattr(cmd_module, "replace_local_auth_list", persist)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/send-local-list",
        json={
            "list_version": 12,
            "update_type": "Full",
            "local_authorization_list": [{"id_tag": "TAG1"}],
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "VersionMismatch"
    persist.assert_not_awaited()


# ---- Reservations ----------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_now_accepted_activates_row(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Accepted"))

    insert_pending = AsyncMock(return_value=8842)
    activate = AsyncMock()
    monkeypatch.setattr(cmd_module, "insert_pending_reservation", insert_pending)
    monkeypatch.setattr(cmd_module, "activate_reservation", activate)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/reserve-now",
        json={
            "connector_id": 1,
            "id_tag": "TAG1",
            "expiry_date": "2026-05-06T15:00:00+00:00",
        },
    )

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "Accepted"
    assert body["reservation_id"] == 8842
    insert_pending.assert_awaited_once()
    activate.assert_awaited_once()


@pytest.mark.asyncio
async def test_reserve_now_rejected_deletes_pending_row(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Occupied"))

    insert_pending = AsyncMock(return_value=8843)
    delete = AsyncMock()
    activate = AsyncMock()
    monkeypatch.setattr(cmd_module, "insert_pending_reservation", insert_pending)
    monkeypatch.setattr(cmd_module, "delete_reservation", delete)
    monkeypatch.setattr(cmd_module, "activate_reservation", activate)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/reserve-now",
        json={
            "connector_id": 1,
            "id_tag": "TAG1",
            "expiry_date": "2026-05-06T15:00:00+00:00",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "Occupied"
    delete.assert_awaited_once()
    activate.assert_not_awaited()


@pytest.mark.asyncio
async def test_reserve_now_invalid_iso_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/reserve-now",
        json={"connector_id": 1, "id_tag": "X", "expiry_date": "not-iso-8601"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_cancel_reservation_persists_on_accepted(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Accepted"))
    persist = AsyncMock()
    monkeypatch.setattr(cmd_module, "repo_cancel_reservation", persist)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/cancel-reservation",
        json={"reservation_id": 8842},
    )

    assert response.status_code == 200
    persist.assert_awaited_once()


@pytest.mark.asyncio
async def test_cancel_reservation_requires_positive_id(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/cancel-reservation",
        json={"reservation_id": 0},
    )
    assert response.status_code == 400


# ---- FirmwareManagement ----------------------------------------------------


@pytest.mark.asyncio
async def test_get_diagnostics_returns_file_name(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(file_name="diag-1.tar.gz"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-diagnostics",
        json={"location": "https://upload.example/diag"},
    )
    assert response.status_code == 200
    assert response.json()["file_name"] == "diag-1.tar.gz"


@pytest.mark.asyncio
async def test_update_firmware_accepts_minimal_body(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status=""))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/update-firmware",
        json={
            "location": "https://fw.example/v2.bin",
            "retrieve_date": "2026-05-07T00:00:00+00:00",
        },
    )
    assert response.status_code == 200
    assert "request_id" in response.json()


@pytest.mark.asyncio
async def test_update_firmware_requires_location(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/update-firmware",
        json={"retrieve_date": "2026-05-07T00:00:00+00:00"},
    )
    assert response.status_code == 400


# ---- Smart Charging --------------------------------------------------------


_SAMPLE_PROFILE = {
    "charging_profile_id": 42,
    "stack_level": 0,
    "charging_profile_purpose": "TxDefaultProfile",
    "charging_profile_kind": "Recurring",
    "charging_schedule": {
        "duration": 3600,
        "charging_rate_unit": "W",
        "charging_schedule_period": [{"start_period": 0, "limit": 11000.0, "number_phases": 3}],
    },
}


@pytest.mark.asyncio
async def test_set_charging_profile_accepted_persists(
    client: httpx.AsyncClient,
    fake_command_service: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    _set_response(fake_command_service, _stub_response(status="Accepted"))
    persist = AsyncMock()
    monkeypatch.setattr(cmd_module, "upsert_charging_profile", persist)

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/set-charging-profile",
        json={"connector_id": 1, "charging_profile": _SAMPLE_PROFILE},
    )
    assert response.status_code == 200
    persist.assert_awaited_once()


@pytest.mark.asyncio
async def test_set_charging_profile_missing_schedule_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/set-charging-profile",
        json={"connector_id": 1, "charging_profile": {"charging_profile_id": 1}},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_clear_charging_profile_all_filters_optional(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(fake_command_service, _stub_response(status="Accepted"))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/clear-charging-profile",
        json={},
    )
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_clear_charging_profile_invalid_purpose_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/clear-charging-profile",
        json={"purpose": "NotARealPurpose"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_composite_schedule_returns_full_shape(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        _stub_response(
            status="Accepted",
            connector_id=1,
            schedule_start="2026-05-06T15:00:00+00:00",
            charging_schedule={
                "duration": 7200,
                "charging_rate_unit": "W",
                "charging_schedule_period": [
                    {"start_period": 0, "limit": 11000.0, "number_phases": 3}
                ],
                "min_charging_rate": None,
                "start_schedule": None,
            },
        ),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-composite-schedule",
        json={"connector_id": 1, "duration": 7200, "charging_rate_unit": "W"},
    )

    body = response.json()
    assert response.status_code == 200
    assert body["status"] == "Accepted"
    assert body["charging_schedule"]["duration"] == 7200
    assert body["charging_schedule"]["charging_schedule_period"][0]["limit"] == 11000.0


@pytest.mark.asyncio
async def test_get_composite_schedule_zero_duration_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-composite-schedule",
        json={"connector_id": 1, "duration": 0},
    )
    assert response.status_code == 400


# ---- Read-only -------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_charger_status_online(
    client: httpx.AsyncClient,
    fake_registry: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from datetime import UTC, datetime

    from eveys_ocpp.api import commands as cmd_module

    fake_registry.get_pod = AsyncMock(return_value="pod-7b3fc9d")
    monkeypatch.setattr(
        cmd_module,
        "get_charge_point_status",
        AsyncMock(return_value=("Available", datetime(2026, 5, 6, 14, 0, tzinfo=UTC))),
    )

    response = await client.get("/api/v1/charge-points/CP_001/commands/get-charger-status")
    body = response.json()
    assert response.status_code == 200
    assert body["online"] is True
    assert body["pod_id"] == "pod-7b3fc9d"
    assert body["last_status"] == "Available"


@pytest.mark.asyncio
async def test_get_charger_status_unknown_404(
    client: httpx.AsyncClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from eveys_ocpp.api import commands as cmd_module

    # Default `fake_registry.get_pod` returns None; pair with a None
    # Postgres lookup to trigger UNKNOWN_CP_ID.
    monkeypatch.setattr(cmd_module, "get_charge_point_status", AsyncMock(return_value=None))

    response = await client.get("/api/v1/charge-points/UNKNOWN/commands/get-charger-status")
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


# ---- Error mapping (exercised via remote-start; helper is shared) ----------


@pytest.mark.asyncio
async def test_unknown_cp_id_maps_to_404(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        GRPCError(Status.NOT_FOUND, "charger CP_X is offline"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_X/commands/remote-start",
        json={"id_tag": "T"},
    )
    assert response.status_code == 404
    assert response.json()["error_code"] == "UNKNOWN_CP_ID"


@pytest.mark.asyncio
async def test_charger_offline_maps_to_503(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        GRPCError(Status.UNAVAILABLE, "charger on different pod, no bus"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_X/commands/remote-start",
        json={"id_tag": "T"},
    )
    assert response.status_code == 503
    assert response.json()["error_code"] == "CHARGER_OFFLINE"


@pytest.mark.asyncio
async def test_charger_timeout_maps_to_504(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        GRPCError(Status.DEADLINE_EXCEEDED, "charger did not reply within 30s"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_X/commands/remote-start",
        json={"id_tag": "T"},
    )
    assert response.status_code == 504
    assert response.json()["error_code"] == "CHARGER_TIMEOUT"


@pytest.mark.asyncio
async def test_grpc_internal_maps_to_500(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        GRPCError(Status.INTERNAL, "boom"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_X/commands/remote-start",
        json={"id_tag": "T"},
    )
    assert response.status_code == 500
    assert response.json()["error_code"] == "INTERNAL_ERROR"


@pytest.mark.asyncio
async def test_invalid_argument_maps_to_400(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    """A GRPCError with INVALID_ARGUMENT (e.g. raised from inside the
    dispatcher when cp_id is empty) maps to 400 BAD_REQUEST."""
    _set_response(
        fake_command_service,
        GRPCError(Status.INVALID_ARGUMENT, "cp_id is required"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_X/commands/remote-start",
        json={"id_tag": "T"},
    )
    assert response.status_code == 400
    assert response.json()["error_code"] == "BAD_REQUEST"


# ---- Body-parsing edge cases ----------------------------------------------


@pytest.mark.asyncio
async def test_non_object_body_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-start",
        content=b'"not an object"',
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_invalid_json_400(client: httpx.AsyncClient) -> None:
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/remote-start",
        content=b"{not-json",
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 400


# ---- TC_079 GetLog (Phase 5 Security) -------------------------------------


@pytest.mark.asyncio
async def test_get_log_security_type_returns_status_and_filename(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    _set_response(
        fake_command_service,
        _stub_response(status="Accepted", filename="security-2026-05-08.tar.gz"),
    )

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-log",
        json={
            "log_type": "SecurityLog",
            "request_id": 42,
            "location": "https://logs.example/incoming",
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "Accepted"
    assert body["file_name"] == "security-2026-05-08.tar.gz"


@pytest.mark.asyncio
async def test_get_log_invalid_log_type_returns_400(
    client: httpx.AsyncClient, fake_command_service: MagicMock
) -> None:
    """`log_type` is closed — anything outside the spec enum is a
    boundary error. A typo would otherwise silently re-route to one
    of the two valid types."""
    _set_response(fake_command_service, _stub_response(status="Accepted", filename=""))

    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-log",
        json={
            "log_type": "AuditLog",  # not a real OCPP value
            "request_id": 1,
            "location": "https://x/",
        },
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_get_log_missing_required_field_returns_400(
    client: httpx.AsyncClient,
) -> None:
    """`location` and `request_id` are required by spec; the boundary
    rejects a missing field."""
    response = await client.post(
        "/api/v1/charge-points/CP_001/commands/get-log",
        json={"log_type": "SecurityLog", "request_id": 1},  # no location
    )
    assert response.status_code == 400
