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
