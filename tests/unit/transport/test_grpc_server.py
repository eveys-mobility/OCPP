"""Unit tests for the gRPC server (E2-4 scaffold + E2-5 RemoteStart + E2-6).

E2-5 lands `RemoteStart` and we verify all four routing branches:
- Charger offline (not in registry) → NOT_FOUND
- Charger online but on a different pod → UNAVAILABLE
- Charger on this pod, charger Accepts → ACCEPTED
- Charger on this pod, charger Rejects → REJECTED

E2-6 lands the remaining six RPCs. Each charger-routed RPC reuses the
same `_dispatch_ocpp_call` helper, so we only re-verify routing edges
on RemoteStart (the original) and instead spot-check each new RPC's
happy path + per-RPC translation logic. `GetChargerStatus` is the
read-only RPC and gets its own coverage.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.grpc_server import OcppGatewayService


@pytest.fixture
def fake_session_factory() -> Any:
    return MagicMock()


@pytest.fixture
def settings() -> Settings:
    return Settings()


# ---- helpers ---------------------------------------------------------------


async def _spawn_server(service: OcppGatewayService) -> tuple[Server, int]:
    """Bind a grpclib server to 127.0.0.1 on an OS-assigned port."""
    server = Server([service])
    await server.start(host="127.0.0.1", port=0)
    sockets = server._server.sockets if server._server else []  # type: ignore[union-attr]
    assert sockets, "server didn't bind"
    return server, sockets[0].getsockname()[1]


# ---- service-class shape ---------------------------------------------------


def test_service_class_implements_every_rpc(fake_session_factory: Any, settings: Settings) -> None:
    """Every RPC the proto defines must be a coroutine on the service class."""
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    expected = {
        "RemoteStart",
        "RemoteStop",
        "Reset",
        "ChangeConfiguration",
        "TriggerMessage",
        "UnlockConnector",
        "GetChargerStatus",
        "GetConfiguration",
        "ClearCache",
        "DataTransfer",
        "GetLocalListVersion",
        "SendLocalList",
        "ReserveNow",
        "CancelReservation",
        "GetDiagnostics",
        "UpdateFirmware",
        "SetChargingProfile",
        "ClearChargingProfile",
        "GetCompositeSchedule",
    }
    for rpc in expected:
        method = getattr(service, rpc, None)
        assert method is not None, f"missing RPC method: {rpc}"
        assert asyncio.iscoroutinefunction(method), f"{rpc} must be async"


def test_settings_grpc_defaults() -> None:
    s = Settings()
    assert s.grpc_host == "0.0.0.0"
    assert s.grpc_port == 50051


# ---- helper for E2-6 happy-path tests --------------------------------------


def _connected_cp(cp_id: str, ocpp_status: str) -> tuple[Any, ConnectionMap]:
    """Build a fake connected EveysChargePoint that replies with the given OCPP status."""
    cp = MagicMock()
    cp.id = cp_id
    response = MagicMock()
    response.status = ocpp_status
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    return cp, cm


# ---- E2-5 RemoteStart -------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_start_charger_offline_returns_not_found(
    fake_session_factory: Any, settings: Settings
) -> None:
    """No connection on this pod, registry says nobody owns it."""
    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value=None)

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
        registry=fake_registry,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="GHOST", id_tag="ABC"))
        assert exc.value.status == Status.NOT_FOUND
        assert "GHOST" in (exc.value.message or "")
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_charger_on_other_pod_no_bus_returns_unavailable(
    fake_session_factory: Any, settings: Settings
) -> None:
    """No bus configured (test fixture) → UNAVAILABLE fallback (pre-E2-10 behaviour).

    With a real bus this branch routes the request cross-pod (see
    ``test_remote_start_charger_on_other_pod_via_bus``).
    """
    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value="pod-other-007")

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
        registry=fake_registry,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="ABC"))
        assert exc.value.status == Status.UNAVAILABLE
        assert "pod-other-007" in (exc.value.message or "")
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_charger_on_other_pod_via_bus(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Registry says another pod owns the WS, bus is configured — round-trip via bus."""
    from eveys_ocpp.bus import BusReply

    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value="pod-other-007")

    fake_bus = MagicMock()
    fake_bus.set_local_dispatcher = MagicMock()
    fake_bus.request = AsyncMock(return_value=BusReply(ok=True, ocpp_status="Accepted"))

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
        registry=fake_registry,
        bus=fake_bus,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStart(
                gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="TAG", connector_id=2)
            )
        assert response.status == gateway_pb2.REMOTE_START_STATUS_ACCEPTED
        fake_bus.request.assert_awaited_once()
        # Inspect the kwargs sent to the bus.
        kwargs = fake_bus.request.await_args.kwargs
        assert kwargs["cp_id"] == "CP_001"
        assert kwargs["owning_pod"] == "pod-other-007"
        assert kwargs["rpc"] == "RemoteStart"
        # The dataclass-derived payload reaches the bus verbatim.
        assert kwargs["payload"]["id_tag"] == "TAG"
        assert kwargs["payload"]["connector_id"] == 2
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_bus_returns_not_found_propagates(
    fake_session_factory: Any, settings: Settings
) -> None:
    """If the owning pod replies NOT_FOUND (charger went offline mid-flight),
    the gRPC caller sees NOT_FOUND too."""
    from eveys_ocpp.bus import BusReply

    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value="pod-other-007")

    fake_bus = MagicMock()
    fake_bus.set_local_dispatcher = MagicMock()
    fake_bus.request = AsyncMock(
        return_value=BusReply(
            ok=False, error_code="NOT_FOUND", error_message="charger went offline"
        )
    )

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
        registry=fake_registry,
        bus=fake_bus,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="X"))
        assert exc.value.status == Status.NOT_FOUND
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_bus_deadline_exceeded_propagates(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Bus says DEADLINE_EXCEEDED → gRPC DEADLINE_EXCEEDED."""
    from eveys_ocpp.bus import BusReply

    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value="pod-other-007")

    fake_bus = MagicMock()
    fake_bus.set_local_dispatcher = MagicMock()
    fake_bus.request = AsyncMock(
        return_value=BusReply(
            ok=False, error_code="DEADLINE_EXCEEDED", error_message="no reply within 30s"
        )
    )

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
        registry=fake_registry,
        bus=fake_bus,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="X"))
        assert exc.value.status == Status.DEADLINE_EXCEEDED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_on_this_pod_accepted(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger on this pod, charger replies Accepted."""
    fake_cp = MagicMock()
    fake_cp.id = "CP_001"
    fake_ocpp_response = MagicMock()
    fake_ocpp_response.status = "Accepted"
    fake_cp.call = AsyncMock(return_value=fake_ocpp_response)

    connections = ConnectionMap()
    connections.add(fake_cp)

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=connections,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStart(
                gateway_pb2.RemoteStartRequest(
                    cp_id="CP_001", id_tag="VALID_RFID_001", connector_id=1
                )
            )
        assert response.status == gateway_pb2.REMOTE_START_STATUS_ACCEPTED
        fake_cp.call.assert_awaited_once()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_on_this_pod_rejected(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger on this pod, charger replies Rejected."""
    fake_cp = MagicMock()
    fake_cp.id = "CP_001"
    fake_ocpp_response = MagicMock()
    fake_ocpp_response.status = "Rejected"
    fake_cp.call = AsyncMock(return_value=fake_ocpp_response)

    connections = ConnectionMap()
    connections.add(fake_cp)

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=connections,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStart(
                gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="X")
            )
        assert response.status == gateway_pb2.REMOTE_START_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_empty_cp_id_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=ConnectionMap(),
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="", id_tag="ABC"))
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_start_charger_timeout_returns_deadline_exceeded(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger on this pod but never replies → DEADLINE_EXCEEDED."""
    # Patch the timeout constant down so the test runs in <1s.
    from eveys_ocpp.transport import grpc_server as gs_module

    monkeypatch.setattr(gs_module, "_OCPP_REQUEST_TIMEOUT_SECONDS", 0.1)

    fake_cp = MagicMock()
    fake_cp.id = "CP_001"

    async def _hang(*_a: object, **_kw: object) -> None:
        await asyncio.sleep(10)  # well past 0.1s timeout

    fake_cp.call = AsyncMock(side_effect=_hang)

    connections = ConnectionMap()
    connections.add(fake_cp)

    service = OcppGatewayService(
        session_factory=fake_session_factory,
        settings=settings,
        connections=connections,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="CP_001", id_tag="X"))
        assert exc.value.status == Status.DEADLINE_EXCEEDED
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 RemoteStop --------------------------------------------------------


@pytest.mark.asyncio
async def test_remote_stop_accepted(fake_session_factory: Any, settings: Settings) -> None:
    cp, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStop(
                gateway_pb2.RemoteStopRequest(cp_id="CP_001", transaction_id=42)
            )
        assert response.status == gateway_pb2.REMOTE_STOP_STATUS_ACCEPTED
        cp.call.assert_awaited_once()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_stop_rejected(fake_session_factory: Any, settings: Settings) -> None:
    _, cm = _connected_cp("CP_001", "Rejected")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStop(
                gateway_pb2.RemoteStopRequest(cp_id="CP_001", transaction_id=42)
            )
        assert response.status == gateway_pb2.REMOTE_STOP_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 Reset -------------------------------------------------------------


@pytest.mark.asyncio
async def test_reset_hard_accepted(fake_session_factory: Any, settings: Settings) -> None:
    cp, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.Reset(
                gateway_pb2.ResetRequest(cp_id="CP_001", type=gateway_pb2.RESET_TYPE_HARD)
            )
        assert response.status == gateway_pb2.RESET_STATUS_ACCEPTED
        # Verify the OCPP request was built with the right type string.
        ocpp_req = cp.call.await_args.args[0]
        assert ocpp_req.type == "Hard"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reset_unspecified_type_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """A client that sends UNSPECIFIED is malformed — fail fast at the boundary."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.Reset(
                    gateway_pb2.ResetRequest(
                        cp_id="CP_001", type=gateway_pb2.RESET_TYPE_UNSPECIFIED
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 ChangeConfiguration -----------------------------------------------


@pytest.mark.asyncio
async def test_change_configuration_reboot_required(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP 1.6 status `RebootRequired` maps to the proto enum."""
    _, cm = _connected_cp("CP_001", "RebootRequired")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ChangeConfiguration(
                gateway_pb2.ChangeConfigurationRequest(
                    cp_id="CP_001", key="HeartbeatInterval", value="60"
                )
            )
        assert response.status == gateway_pb2.CHANGE_CONFIGURATION_STATUS_REBOOT_REQUIRED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_change_configuration_empty_key_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.ChangeConfiguration(
                    gateway_pb2.ChangeConfigurationRequest(cp_id="CP_001", key="", value="x")
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 TriggerMessage ----------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_message_heartbeat_accepted(
    fake_session_factory: Any, settings: Settings
) -> None:
    cp, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.TriggerMessage(
                gateway_pb2.TriggerMessageRequest(
                    cp_id="CP_001",
                    requested_message=gateway_pb2.TRIGGER_MESSAGE_TYPE_HEARTBEAT,
                )
            )
        assert response.status == gateway_pb2.TRIGGER_MESSAGE_STATUS_ACCEPTED
        ocpp_req = cp.call.await_args.args[0]
        assert ocpp_req.requested_message == "Heartbeat"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_trigger_message_unspecified_kind_rejected_at_boundary(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.TriggerMessage(
                    gateway_pb2.TriggerMessageRequest(
                        cp_id="CP_001",
                        requested_message=gateway_pb2.TRIGGER_MESSAGE_TYPE_UNSPECIFIED,
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_trigger_message_not_implemented_status(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP `NotImplemented` flows through to the proto enum."""
    _, cm = _connected_cp("CP_001", "NotImplemented")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.TriggerMessage(
                gateway_pb2.TriggerMessageRequest(
                    cp_id="CP_001",
                    requested_message=gateway_pb2.TRIGGER_MESSAGE_TYPE_BOOT_NOTIFICATION,
                )
            )
        assert response.status == gateway_pb2.TRIGGER_MESSAGE_STATUS_NOT_IMPLEMENTED
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 UnlockConnector ---------------------------------------------------


@pytest.mark.asyncio
async def test_unlock_connector_unlocked(fake_session_factory: Any, settings: Settings) -> None:
    _, cm = _connected_cp("CP_001", "Unlocked")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.UnlockConnector(
                gateway_pb2.UnlockConnectorRequest(cp_id="CP_001", connector_id=2)
            )
        assert response.status == gateway_pb2.UNLOCK_CONNECTOR_STATUS_UNLOCKED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unlock_connector_zero_connector_invalid(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP UnlockConnector requires a specific connector — connector_id=0 invalid."""
    _, cm = _connected_cp("CP_001", "Unlocked")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.UnlockConnector(
                    gateway_pb2.UnlockConnectorRequest(cp_id="CP_001", connector_id=0)
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_unlock_connector_unlock_failed(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "UnlockFailed")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.UnlockConnector(
                gateway_pb2.UnlockConnectorRequest(cp_id="CP_001", connector_id=1)
            )
        assert response.status == gateway_pb2.UNLOCK_CONNECTOR_STATUS_UNLOCK_FAILED
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-6 GetChargerStatus (read-only; no OCPP round-trip) -----------------


@pytest.mark.asyncio
async def test_get_charger_status_online_no_db_row(settings: Settings) -> None:
    """Charger online (registry has the key) but no Postgres row yet."""
    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value="pod-A")

    # Patch get_charge_point_status to return None (charger never booted).
    from eveys_ocpp.transport import grpc_server as gs_module

    async def _no_row(*_a: object, **_kw: object) -> None:
        return None

    # Build a session_factory that returns a context-manager-yielding mock.
    class _Ctx:
        async def __aenter__(self) -> Any:
            return AsyncMock()

        async def __aexit__(self, *exc: object) -> None:
            return None

    sf = MagicMock(side_effect=lambda: _Ctx())

    service = OcppGatewayService(session_factory=sf, settings=settings, registry=fake_registry)
    # Monkey-patch the imported helper directly on the module.
    original = gs_module.get_charge_point_status
    gs_module.get_charge_point_status = _no_row  # type: ignore[assignment]
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetChargerStatus(
                gateway_pb2.GetChargerStatusRequest(cp_id="CP_001")
            )
        assert response.cp_id == "CP_001"
        assert response.online is True
        assert response.pod_id == "pod-A"
        assert response.last_status == ""
        assert response.last_heartbeat_at == ""
    finally:
        gs_module.get_charge_point_status = original  # type: ignore[assignment]
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_charger_status_offline_with_db_row(settings: Settings) -> None:
    """Registry has no key but Postgres has the row → offline + cached state."""
    from datetime import UTC, datetime

    from eveys_ocpp.transport import grpc_server as gs_module

    fake_registry = AsyncMock()
    fake_registry.get_pod = AsyncMock(return_value=None)

    hb = datetime(2026, 4, 30, 12, 0, 0, tzinfo=UTC)

    async def _row(*_a: object, **_kw: object) -> tuple[str, datetime]:
        return ("Charging", hb)

    class _Ctx:
        async def __aenter__(self) -> Any:
            return AsyncMock()

        async def __aexit__(self, *exc: object) -> None:
            return None

    sf = MagicMock(side_effect=lambda: _Ctx())

    service = OcppGatewayService(session_factory=sf, settings=settings, registry=fake_registry)
    original = gs_module.get_charge_point_status
    gs_module.get_charge_point_status = _row  # type: ignore[assignment]
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetChargerStatus(
                gateway_pb2.GetChargerStatusRequest(cp_id="CP_001")
            )
        assert response.online is False
        assert response.pod_id == ""
        assert response.last_status == "Charging"
        assert response.last_heartbeat_at == hb.isoformat()
    finally:
        gs_module.get_charge_point_status = original  # type: ignore[assignment]
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_charger_status_empty_cp_id_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetChargerStatus(gateway_pb2.GetChargerStatusRequest(cp_id=""))
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1A GetConfiguration -------------------------------------------------


def _connected_cp_with_config(
    cp_id: str,
    *,
    keys: list[dict[str, Any]],
    unknown: list[str],
) -> tuple[Any, ConnectionMap]:
    """`GetConfiguration` returns lists, not a single status. This
    helper mirrors `_connected_cp` but lets us configure the lists
    directly."""
    cp = MagicMock()
    cp.id = cp_id
    response = MagicMock()
    response.configuration_key = keys
    response.unknown_key = unknown
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    return cp, cm


@pytest.mark.asyncio
async def test_get_configuration_returns_keys_and_unknown(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger returns two known keys (one readonly) plus one unknown.
    The gateway translates the dict shape to typed proto messages."""
    _, cm = _connected_cp_with_config(
        "CP_001",
        keys=[
            {"key": "HeartbeatInterval", "readonly": False, "value": "60"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "2"},
        ],
        unknown=["NoSuchKey"],
    )
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetConfiguration(
                gateway_pb2.GetConfigurationRequest(
                    cp_id="CP_001", keys=["HeartbeatInterval", "NumberOfConnectors", "NoSuchKey"]
                )
            )
        assert len(response.configuration_key) == 2
        assert response.configuration_key[0].key == "HeartbeatInterval"
        assert response.configuration_key[0].readonly is False
        assert response.configuration_key[0].value == "60"
        assert response.configuration_key[1].key == "NumberOfConnectors"
        assert response.configuration_key[1].readonly is True
        assert list(response.unknown_key) == ["NoSuchKey"]
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_configuration_empty_keys_means_all(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Empty `keys` in the proto request → forwarded as `None` to the
    OCPP dataclass, which the spec interprets as 'return everything'."""
    cp, cm = _connected_cp_with_config("CP_001", keys=[], unknown=[])
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            await stub.GetConfiguration(gateway_pb2.GetConfigurationRequest(cp_id="CP_001"))
        # The OCPP request the charger received had key=None (per spec
        # — empty list != "all"; dataclass default is None).
        cp.call.assert_awaited_once()
        sent = cp.call.await_args.args[0]
        assert sent.key is None
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_configuration_handles_missing_value_field(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP dataclass marks `value` optional; if a charger omits it
    the gateway must still return a valid proto (empty string, not
    a translation crash)."""
    _, cm = _connected_cp_with_config(
        "CP_001",
        keys=[{"key": "WriteOnlyKey", "readonly": False}],
        unknown=[],
    )
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetConfiguration(
                gateway_pb2.GetConfigurationRequest(cp_id="CP_001", keys=["WriteOnlyKey"])
            )
        assert response.configuration_key[0].value == ""
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1A ClearCache -------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_cache_accepted(fake_session_factory: Any, settings: Settings) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ClearCache(gateway_pb2.ClearCacheRequest(cp_id="CP_001"))
        assert response.status == gateway_pb2.CLEAR_CACHE_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_clear_cache_rejected(fake_session_factory: Any, settings: Settings) -> None:
    _, cm = _connected_cp("CP_001", "Rejected")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ClearCache(gateway_pb2.ClearCacheRequest(cp_id="CP_001"))
        assert response.status == gateway_pb2.CLEAR_CACHE_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1A DataTransfer (CSMS → charger) ------------------------------------


@pytest.mark.asyncio
async def test_data_transfer_accepted_with_reply(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Happy path: charger accepts and returns a vendor payload."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    response.data = '{"reply":"ok"}'
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            grpc_response = await stub.DataTransfer(
                gateway_pb2.DataTransferRequest(
                    cp_id="CP_001",
                    vendor_id="acme.fastcharge",
                    message_id="ping",
                    data='{"hi":1}',
                )
            )
        assert grpc_response.status == gateway_pb2.DATA_TRANSFER_STATUS_ACCEPTED
        assert grpc_response.data == '{"reply":"ok"}'
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_data_transfer_unknown_vendor_id_status(
    fake_session_factory: Any, settings: Settings
) -> None:
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "UnknownVendorId"
    response.data = None
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            grpc_response = await stub.DataTransfer(
                gateway_pb2.DataTransferRequest(cp_id="CP_001", vendor_id="unknown.vendor")
            )
        assert grpc_response.status == gateway_pb2.DATA_TRANSFER_STATUS_UNKNOWN_VENDOR_ID
        assert grpc_response.data == ""
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_data_transfer_empty_vendor_id_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """`vendor_id` is required by OCPP — gateway boundary rejects
    empty before sending to the charger."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.DataTransfer(
                    gateway_pb2.DataTransferRequest(cp_id="CP_001", vendor_id="")
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1B GetLocalListVersion ----------------------------------------------


@pytest.mark.asyncio
async def test_get_local_list_version_returns_charger_value(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Round-trips through the OCPP layer; charger is the source of
    truth (gateway-side mirror is for operator queries, not this
    RPC)."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.list_version = 42
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            grpc_response = await stub.GetLocalListVersion(
                gateway_pb2.GetLocalListVersionRequest(cp_id="CP_001")
            )
        assert grpc_response.list_version == 42
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_local_list_version_negative_one_when_charger_has_no_list(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP spec: charger returns `-1` when it has no list. The
    gateway forwards that integer verbatim."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.list_version = -1
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            grpc_response = await stub.GetLocalListVersion(
                gateway_pb2.GetLocalListVersionRequest(cp_id="CP_001")
            )
        assert grpc_response.list_version == -1
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1B SendLocalList ----------------------------------------------------


@pytest.mark.asyncio
async def test_send_local_list_full_accepted_persists_mirror(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Full update + charger Accepted → gateway-side mirror is replaced
    via `replace_local_auth_list`. Differential path is NOT taken."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    replace_calls: list[dict[str, Any]] = []
    differential_calls: list[dict[str, Any]] = []

    async def fake_replace(_session: Any, **kwargs: Any) -> None:
        replace_calls.append(kwargs)

    async def fake_differential(_session: Any, **kwargs: Any) -> None:
        differential_calls.append(kwargs)

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.replace_local_auth_list", fake_replace)
    monkeypatch.setattr(
        "eveys_ocpp.transport.grpc_server.apply_local_auth_list_differential",
        fake_differential,
    )

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SendLocalList(
                gateway_pb2.SendLocalListRequest(
                    cp_id="CP_001",
                    list_version=5,
                    update_type=gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_FULL,
                    local_authorization_list=[
                        gateway_pb2.AuthorizationData(
                            id_tag="TAG_A",
                            id_tag_info=gateway_pb2.IdTagInfo(
                                status=gateway_pb2.AUTHORIZATION_STATUS_ACCEPTED,
                                parent_id_tag="PARENT",
                            ),
                        )
                    ],
                )
            )
        assert response.status == gateway_pb2.SEND_LOCAL_LIST_STATUS_ACCEPTED
        # Full replace path was taken with the right list version.
        assert len(replace_calls) == 1
        assert replace_calls[0]["cp_id"] == "CP_001"
        assert replace_calls[0]["list_version"] == 5
        assert replace_calls[0]["entries"][0]["id_tag"] == "TAG_A"
        assert replace_calls[0]["entries"][0]["id_tag_info"]["status"] == "Accepted"
        # Differential was NOT touched.
        assert differential_calls == []
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_local_list_differential_accepted_routes_to_differential(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Differential update + charger Accepted → gateway-side mirror
    is updated via `apply_local_auth_list_differential`."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    differential_calls: list[dict[str, Any]] = []

    async def fake_differential(_session: Any, **kwargs: Any) -> None:
        differential_calls.append(kwargs)

    async def fake_replace(_session: Any, **kwargs: Any) -> None:
        raise AssertionError("Full replace must not run on Differential update")

    monkeypatch.setattr(
        "eveys_ocpp.transport.grpc_server.apply_local_auth_list_differential",
        fake_differential,
    )
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.replace_local_auth_list", fake_replace)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            # Differential with one delete (no id_tag_info) and one upsert.
            response = await stub.SendLocalList(
                gateway_pb2.SendLocalListRequest(
                    cp_id="CP_001",
                    list_version=6,
                    update_type=gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_DIFFERENTIAL,
                    local_authorization_list=[
                        gateway_pb2.AuthorizationData(id_tag="TAG_DEL"),
                        gateway_pb2.AuthorizationData(
                            id_tag="TAG_UPSERT",
                            id_tag_info=gateway_pb2.IdTagInfo(
                                status=gateway_pb2.AUTHORIZATION_STATUS_ACCEPTED
                            ),
                        ),
                    ],
                )
            )
        assert response.status == gateway_pb2.SEND_LOCAL_LIST_STATUS_ACCEPTED
        assert len(differential_calls) == 1
        entries = differential_calls[0]["entries"]
        # Delete entry: id_tag_info must be None on the wire shape.
        del_entry = next(e for e in entries if e["id_tag"] == "TAG_DEL")
        assert del_entry["id_tag_info"] is None
        # Upsert entry: id_tag_info populated.
        ups_entry = next(e for e in entries if e["id_tag"] == "TAG_UPSERT")
        assert ups_entry["id_tag_info"]["status"] == "Accepted"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_local_list_version_mismatch_does_not_persist(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If the charger replies VersionMismatch, the gateway-side mirror
    is NOT updated — the charger is the source of truth and we'd
    create drift if we mirrored a rejected update.
    """
    _, cm = _connected_cp("CP_001", "VersionMismatch")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    persist_calls: list[Any] = []

    async def boom(*args: Any, **kwargs: Any) -> None:
        persist_calls.append((args, kwargs))

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.replace_local_auth_list", boom)
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.apply_local_auth_list_differential", boom)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SendLocalList(
                gateway_pb2.SendLocalListRequest(
                    cp_id="CP_001",
                    list_version=5,
                    update_type=gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_FULL,
                )
            )
        assert response.status == gateway_pb2.SEND_LOCAL_LIST_STATUS_VERSION_MISMATCH
        assert persist_calls == []  # nothing got mirrored
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_local_list_persist_failure_still_returns_accepted(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Persist failure after a successful charger update is logged
    but does not promote to a gRPC error. The caller's OCPP-level
    SUCCESS is real; misleading them into thinking the list isn't on
    the charger would be worse than divergence (which a subsequent
    GetLocalListVersion surfaces)."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    async def fake_replace_raises(*args: Any, **kwargs: Any) -> None:
        raise RuntimeError("DB blew up")

    monkeypatch.setattr(
        "eveys_ocpp.transport.grpc_server.replace_local_auth_list", fake_replace_raises
    )

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SendLocalList(
                gateway_pb2.SendLocalListRequest(
                    cp_id="CP_001",
                    list_version=5,
                    update_type=gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_FULL,
                )
            )
        assert response.status == gateway_pb2.SEND_LOCAL_LIST_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_local_list_unspecified_update_type_invalid(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Boundary validation: the proto's zero-value
    `LOCAL_AUTH_LIST_UPDATE_TYPE_UNSPECIFIED` is rejected so a
    misconfigured caller doesn't reach the OCPP layer."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SendLocalList(
                    gateway_pb2.SendLocalListRequest(
                        cp_id="CP_001",
                        list_version=1,
                        # update_type defaults to UNSPECIFIED (= 0).
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_send_local_list_unspecified_authorization_status_invalid(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Each entry's `id_tag_info.status` must be a defined enum.
    The translator raises INVALID_ARGUMENT before the charger sees
    a malformed call."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SendLocalList(
                    gateway_pb2.SendLocalListRequest(
                        cp_id="CP_001",
                        list_version=1,
                        update_type=gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_FULL,
                        local_authorization_list=[
                            gateway_pb2.AuthorizationData(
                                id_tag="X",
                                # status defaults to UNSPECIFIED.
                                id_tag_info=gateway_pb2.IdTagInfo(),
                            )
                        ],
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1C ReserveNow / CancelReservation -----------------------------------


@pytest.mark.asyncio
async def test_reserve_now_accepted_assigns_id_and_activates(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Happy path: gateway allocates a reservation_id by inserting a
    Pending row, charger Accepts, gateway flips it to Active. The
    response carries the assigned ID so the caller can use it later
    for CancelReservation."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    insert_calls: list[dict[str, Any]] = []
    activate_calls: list[int] = []
    delete_calls: list[int] = []

    async def fake_insert(_session: Any, **kwargs: Any) -> int:
        insert_calls.append(kwargs)
        return 777

    async def fake_activate(_session: Any, *, reservation_id: int) -> None:
        activate_calls.append(reservation_id)

    async def fake_delete(_session: Any, *, reservation_id: int) -> None:
        delete_calls.append(reservation_id)

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.insert_pending_reservation", fake_insert)
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.activate_reservation", fake_activate)
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.delete_reservation", fake_delete)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ReserveNow(
                gateway_pb2.ReserveNowRequest(
                    cp_id="CP_001",
                    connector_id=2,
                    expiry_date="2026-12-31T23:59:59+00:00",
                    id_tag="TAG_VIP",
                    parent_id_tag="FAMILY_1",
                )
            )
        assert response.status == gateway_pb2.RESERVE_NOW_STATUS_ACCEPTED
        assert response.reservation_id == 777
        # Pending row was inserted with the operator's metadata.
        assert insert_calls[0]["cp_id"] == "CP_001"
        assert insert_calls[0]["connector_id"] == 2
        assert insert_calls[0]["id_tag"] == "TAG_VIP"
        assert insert_calls[0]["parent_id_tag"] == "FAMILY_1"
        # Active flip happened with the assigned id; no delete.
        assert activate_calls == [777]
        assert delete_calls == []
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reserve_now_charger_occupied_drops_pending_row(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger reports Occupied → the Pending row is deleted (it
    never came alive on the charger). The response still carries the
    allocated ID for caller-side correlation."""
    _, cm = _connected_cp("CP_001", "Occupied")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    activate_calls: list[int] = []
    delete_calls: list[int] = []

    async def fake_insert(_session: Any, **_kw: Any) -> int:
        return 555

    async def fake_activate(_session: Any, *, reservation_id: int) -> None:
        activate_calls.append(reservation_id)

    async def fake_delete(_session: Any, *, reservation_id: int) -> None:
        delete_calls.append(reservation_id)

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.insert_pending_reservation", fake_insert)
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.activate_reservation", fake_activate)
    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.delete_reservation", fake_delete)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ReserveNow(
                gateway_pb2.ReserveNowRequest(
                    cp_id="CP_001",
                    connector_id=1,
                    expiry_date="2026-12-31T23:59:59+00:00",
                    id_tag="TAG",
                )
            )
        assert response.status == gateway_pb2.RESERVE_NOW_STATUS_OCCUPIED
        assert response.reservation_id == 555
        assert activate_calls == []
        assert delete_calls == [555]
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reserve_now_invalid_expiry_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Malformed `expiry_date` is caught at the gateway boundary
    before the Pending row is inserted (no orphan row from a bad
    operator request)."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.ReserveNow(
                    gateway_pb2.ReserveNowRequest(
                        cp_id="CP_001",
                        connector_id=1,
                        expiry_date="not-a-date",
                        id_tag="TAG",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_reserve_now_empty_id_tag_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.ReserveNow(
                    gateway_pb2.ReserveNowRequest(
                        cp_id="CP_001",
                        connector_id=1,
                        expiry_date="2026-12-31T23:59:59+00:00",
                        id_tag="",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_cancel_reservation_accepted_marks_cancelled(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger Accepts → mirror is marked Cancelled."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    cancel_calls: list[int] = []

    async def fake_cancel(_session: Any, *, reservation_id: int) -> bool:
        cancel_calls.append(reservation_id)
        return True

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.cancel_reservation", fake_cancel)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.CancelReservation(
                gateway_pb2.CancelReservationRequest(cp_id="CP_001", reservation_id=99)
            )
        assert response.status == gateway_pb2.CANCEL_RESERVATION_STATUS_ACCEPTED
        assert cancel_calls == [99]
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_cancel_reservation_rejected_does_not_persist(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger Rejects (already expired / consumed / unknown) → mirror
    is left alone per ADR-0021. The charger's view wins."""
    _, cm = _connected_cp("CP_001", "Rejected")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    async def boom(*args: Any, **kwargs: Any) -> bool:
        raise AssertionError("must not call cancel_reservation on Rejected reply")

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.cancel_reservation", boom)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.CancelReservation(
                gateway_pb2.CancelReservationRequest(cp_id="CP_001", reservation_id=99)
            )
        assert response.status == gateway_pb2.CANCEL_RESERVATION_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_cancel_reservation_zero_id_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.CancelReservation(
                    gateway_pb2.CancelReservationRequest(cp_id="CP_001", reservation_id=0)
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1F GetDiagnostics / UpdateFirmware ---------------------------------


@pytest.mark.asyncio
async def test_get_diagnostics_returns_charger_filename(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger replies with a chosen `file_name`; gateway forwards verbatim."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.file_name = "diag-2026-05-05.tar.gz"
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.GetDiagnostics(
                gateway_pb2.GetDiagnosticsRequest(
                    cp_id="CP_001",
                    location="https://logs.eveys.example/incoming",
                )
            )
        assert response_grpc.file_name == "diag-2026-05-05.tar.gz"
        # Optional fields default-zero on the proto side flow through
        # as `None` to the OCPP dataclass (charger then uses defaults).
        sent = cp.call.await_args.args[0]
        assert sent.location == "https://logs.eveys.example/incoming"
        assert sent.retries is None
        assert sent.start_time is None
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_diagnostics_handles_optional_filename_missing(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP marks `file_name` optional; an absent value coerces to ''."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.file_name = None
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.GetDiagnostics(
                gateway_pb2.GetDiagnosticsRequest(cp_id="CP_001", location="ftp://logs/")
            )
        assert response_grpc.file_name == ""
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_diagnostics_empty_location_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetDiagnostics(
                    gateway_pb2.GetDiagnosticsRequest(cp_id="CP_001", location="")
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_update_firmware_returns_empty_response(
    fake_session_factory: Any, settings: Settings
) -> None:
    """OCPP UpdateFirmware.conf carries no fields; the gRPC response
    is empty too. Verify the OCPP request was assembled correctly."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()  # empty response — no fields to set
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.UpdateFirmware(
                gateway_pb2.UpdateFirmwareRequest(
                    cp_id="CP_001",
                    location="https://firmware.eveys.example/2026.05.bin",
                    retrieve_date="2026-05-05T03:00:00+00:00",
                )
            )
        # Empty proto message — no fields to assert beyond the type.
        assert isinstance(response_grpc, gateway_pb2.UpdateFirmwareResponse)
        sent = cp.call.await_args.args[0]
        assert sent.location == "https://firmware.eveys.example/2026.05.bin"
        assert sent.retrieve_date == "2026-05-05T03:00:00+00:00"
        assert sent.retries is None
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_update_firmware_empty_location_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.UpdateFirmware(
                    gateway_pb2.UpdateFirmwareRequest(
                        cp_id="CP_001",
                        location="",
                        retrieve_date="2026-05-05T03:00:00+00:00",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_update_firmware_empty_retrieve_date_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.UpdateFirmware(
                    gateway_pb2.UpdateFirmwareRequest(
                        cp_id="CP_001",
                        location="https://firmware/",
                        retrieve_date="",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- E2-1E SetChargingProfile / ClearChargingProfile / GetCompositeSchedule -


def _make_proto_charging_profile(
    profile_id: int = 1,
) -> gateway_pb2.ChargingProfile:
    """Build a minimal valid proto ChargingProfile for use in tests."""
    return gateway_pb2.ChargingProfile(
        charging_profile_id=profile_id,
        stack_level=0,
        charging_profile_purpose=gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_DEFAULT_PROFILE,
        charging_profile_kind=gateway_pb2.CHARGING_PROFILE_KIND_ABSOLUTE,
        charging_schedule=gateway_pb2.ChargingSchedule(
            duration=3600,
            charging_rate_unit=gateway_pb2.CHARGING_RATE_UNIT_W,
            charging_schedule_period=[
                gateway_pb2.ChargingSchedulePeriod(start_period=0, limit=11000.0, number_phases=3),
                gateway_pb2.ChargingSchedulePeriod(
                    start_period=1800, limit=7400.0, number_phases=3
                ),
            ],
        ),
    )


@pytest.mark.asyncio
async def test_set_charging_profile_accepted_persists_mirror(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger Accepts → upsert into mirror with the wire-shape dict."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    upsert_calls: list[dict[str, Any]] = []

    async def fake_upsert(_session: Any, **kwargs: Any) -> int:
        upsert_calls.append(kwargs)
        return 100

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.upsert_charging_profile", fake_upsert)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SetChargingProfile(
                gateway_pb2.SetChargingProfileRequest(
                    cp_id="CP_001",
                    connector_id=1,
                    cs_charging_profiles=_make_proto_charging_profile(profile_id=42),
                )
            )
        assert response.status == gateway_pb2.CHARGING_PROFILE_STATUS_ACCEPTED
        assert len(upsert_calls) == 1
        assert upsert_calls[0]["cp_id"] == "CP_001"
        assert upsert_calls[0]["connector_id"] == 1
        assert upsert_calls[0]["profile"]["charging_profile_id"] == 42
        # Purpose lands as the OCPP wire string ("TxDefaultProfile")
        # — the repo column is String.
        assert upsert_calls[0]["profile"]["charging_profile_purpose"] == "TxDefaultProfile"
        assert len(upsert_calls[0]["schedule_periods"]) == 2
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_set_charging_profile_rejected_does_not_persist(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger Rejects → no mirror write (gateway state stays
    consistent with what the charger has)."""
    _, cm = _connected_cp("CP_001", "Rejected")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    async def boom(*args: Any, **kwargs: Any) -> int:
        raise AssertionError("must not call upsert on Rejected reply")

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.upsert_charging_profile", boom)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SetChargingProfile(
                gateway_pb2.SetChargingProfileRequest(
                    cp_id="CP_001",
                    connector_id=1,
                    cs_charging_profiles=_make_proto_charging_profile(),
                )
            )
        assert response.status == gateway_pb2.CHARGING_PROFILE_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_set_charging_profile_missing_profile_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """`cs_charging_profiles` is required by spec; gateway boundary
    rejects when the sub-message is unset."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SetChargingProfile(
                    gateway_pb2.SetChargingProfileRequest(cp_id="CP_001", connector_id=1)
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_set_charging_profile_unspecified_purpose_invalid(
    fake_session_factory: Any, settings: Settings
) -> None:
    """A profile with `purpose=UNSPECIFIED` would build a malformed
    OCPP call; reject at the boundary."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    bad_profile = _make_proto_charging_profile()
    bad_profile.charging_profile_purpose = gateway_pb2.CHARGING_PROFILE_PURPOSE_UNSPECIFIED
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SetChargingProfile(
                    gateway_pb2.SetChargingProfileRequest(
                        cp_id="CP_001",
                        connector_id=1,
                        cs_charging_profiles=bad_profile,
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_clear_charging_profile_accepted_clears_mirror(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger Accepts → mirror flips matching rows to Cleared. Filter
    fields lower from proto-zero to None correctly."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    clear_calls: list[dict[str, Any]] = []

    async def fake_clear(_session: Any, **kwargs: Any) -> int:
        clear_calls.append(kwargs)
        return 2

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.clear_charging_profiles", fake_clear)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ClearChargingProfile(
                gateway_pb2.ClearChargingProfileRequest(
                    cp_id="CP_001",
                    charging_profile_purpose=(gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_PROFILE),
                )
            )
        assert response.status == gateway_pb2.CLEAR_CHARGING_PROFILE_STATUS_ACCEPTED
        # Repo got the string-name purpose; other filters None.
        assert clear_calls[0]["purpose"] == "TxProfile"
        assert clear_calls[0]["profile_id"] is None
        assert clear_calls[0]["connector_id"] is None
        assert clear_calls[0]["stack_level"] is None
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_clear_charging_profile_unknown_does_not_persist(
    fake_session_factory: Any, settings: Settings, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Charger reports Unknown (no matching profile) → no mirror update."""
    _, cm = _connected_cp("CP_001", "Unknown")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )

    async def boom(*args: Any, **kwargs: Any) -> int:
        raise AssertionError("must not call clear on Unknown reply")

    monkeypatch.setattr("eveys_ocpp.transport.grpc_server.clear_charging_profiles", boom)

    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ClearChargingProfile(
                gateway_pb2.ClearChargingProfileRequest(cp_id="CP_001")
            )
        assert response.status == gateway_pb2.CLEAR_CHARGING_PROFILE_STATUS_UNKNOWN
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_composite_schedule_translates_charger_reply(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger reply (status, connector_id, schedule_start, charging_schedule
    as OCPP wire dict) → typed proto response."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    response.connector_id = 1
    response.schedule_start = "2026-12-31T22:00:00+00:00"
    response.charging_schedule = {
        "duration": 7200,
        "charging_rate_unit": "W",
        "min_charging_rate": 1000.0,
        "start_schedule": "2026-12-31T22:00:00+00:00",
        "charging_schedule_period": [
            {"start_period": 0, "limit": 11000.0, "number_phases": 3},
            {"start_period": 3600, "limit": 7400.0},
        ],
    }
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.GetCompositeSchedule(
                gateway_pb2.GetCompositeScheduleRequest(
                    cp_id="CP_001",
                    connector_id=1,
                    duration=7200,
                    charging_rate_unit=gateway_pb2.CHARGING_RATE_UNIT_W,
                )
            )
        assert response_grpc.status == gateway_pb2.GET_COMPOSITE_SCHEDULE_STATUS_ACCEPTED
        assert response_grpc.connector_id == 1
        assert response_grpc.schedule_start == "2026-12-31T22:00:00+00:00"
        assert response_grpc.charging_schedule.duration == 7200
        assert (
            response_grpc.charging_schedule.charging_rate_unit == gateway_pb2.CHARGING_RATE_UNIT_W
        )
        assert len(response_grpc.charging_schedule.charging_schedule_period) == 2
        assert response_grpc.charging_schedule.charging_schedule_period[0].limit == 11000.0
        assert response_grpc.charging_schedule.charging_schedule_period[1].start_period == 3600
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_composite_schedule_zero_duration_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetCompositeSchedule(
                    gateway_pb2.GetCompositeScheduleRequest(
                        cp_id="CP_001", connector_id=1, duration=0
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_composite_schedule_rejected_passes_through(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger Rejects → status maps to proto enum; schedule fields
    default-empty without crashing the translator."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Rejected"
    response.connector_id = 0
    response.schedule_start = None
    response.charging_schedule = None
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.GetCompositeSchedule(
                gateway_pb2.GetCompositeScheduleRequest(
                    cp_id="CP_001", connector_id=1, duration=3600
                )
            )
        assert response_grpc.status == gateway_pb2.GET_COMPOSITE_SCHEDULE_STATUS_REJECTED
        assert response_grpc.schedule_start == ""
    finally:
        server.close()
        await server.wait_closed()
