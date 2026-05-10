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
import datetime as _dt
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio as _pytest_asyncio
from cryptography import x509 as _x509
from cryptography.hazmat.primitives import hashes as _hashes
from cryptography.hazmat.primitives import serialization as _ser
from cryptography.hazmat.primitives.asymmetric import rsa as _rsa
from cryptography.x509.oid import NameOID as _NameOID
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server
from sqlalchemy.ext.asyncio import async_sessionmaker as _amk
from sqlalchemy.ext.asyncio import create_async_engine as _create_async_engine

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.persistence.models import Base as _Base
from eveys_ocpp.persistence.models import ChargePoint as _ChargePoint
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
        "ChangeAvailability",
        "ExtendedTriggerMessage",
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


# ---- TC_079 GetLog (Phase 5 Security) -------------------------------------


@pytest.mark.asyncio
async def test_get_log_security_type_round_trips(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger replies Accepted with a chosen filename; gateway
    forwards verbatim. The closed `log_type` enum is the load-bearing
    field — operators issue type=SECURITY to retrieve audit log."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    response.filename = "security-2026-05-08.tar.gz"
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
            response_grpc = await stub.GetLog(
                gateway_pb2.GetLogRequest(
                    cp_id="CP_001",
                    log_type=gateway_pb2.LOG_TYPE_SECURITY,
                    request_id=42,
                    location="https://logs.eveys.example/incoming",
                )
            )
        assert response_grpc.status == gateway_pb2.LOG_STATUS_ACCEPTED
        assert response_grpc.file_name == "security-2026-05-08.tar.gz"
        # The OCPP call must carry the spec's `SecurityLog` enum value
        # — without it, the charger sends the diagnostics log instead,
        # which silently breaks audit retrieval.
        sent = cp.call.await_args.args[0]
        from ocpp.v16 import enums as ocpp_enums

        assert sent.log_type == ocpp_enums.Log.security_log
        assert sent.request_id == 42
        # `log` is the spec's Dict; remoteLocation is required.
        assert sent.log["remoteLocation"] == "https://logs.eveys.example/incoming"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_diagnostics_type_uses_diagnostics_log_enum(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Operators can also use GetLog for diagnostics (sibling of
    GetDiagnostics — different RPC, same upload). The proto
    LOG_TYPE_DIAGNOSTICS must map to the OCPP `DiagnosticsLog`
    enum, never silently re-route as security."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    response.filename = ""
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
            await stub.GetLog(
                gateway_pb2.GetLogRequest(
                    cp_id="CP_001",
                    log_type=gateway_pb2.LOG_TYPE_DIAGNOSTICS,
                    request_id=7,
                    location="https://x/",
                )
            )
        from ocpp.v16 import enums as ocpp_enums

        sent = cp.call.await_args.args[0]
        assert sent.log_type == ocpp_enums.Log.diagnostics_log
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_optional_timestamps_propagate_when_provided(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Optional time-window filters land on the OCPP `log` dict's
    `oldestTimestamp` / `latestTimestamp` keys (per spec)."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    response.filename = ""
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
            await stub.GetLog(
                gateway_pb2.GetLogRequest(
                    cp_id="CP_001",
                    log_type=gateway_pb2.LOG_TYPE_SECURITY,
                    request_id=1,
                    location="https://x/",
                    oldest_timestamp="2026-05-01T00:00:00Z",
                    latest_timestamp="2026-05-08T00:00:00Z",
                )
            )
        sent = cp.call.await_args.args[0]
        assert sent.log["oldestTimestamp"] == "2026-05-01T00:00:00Z"
        assert sent.log["latestTimestamp"] == "2026-05-08T00:00:00Z"
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_rejected_status_maps_to_proto_enum(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger `Rejected` → proto `LOG_STATUS_REJECTED`."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Rejected"
    response.filename = ""
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
            response_grpc = await stub.GetLog(
                gateway_pb2.GetLogRequest(
                    cp_id="CP_001",
                    log_type=gateway_pb2.LOG_TYPE_SECURITY,
                    request_id=1,
                    location="https://x/",
                )
            )
        assert response_grpc.status == gateway_pb2.LOG_STATUS_REJECTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_accepted_canceled_maps_to_proto_enum(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger `AcceptedCanceled` (new request accepted, prior in-
    flight upload cancelled) → proto LOG_STATUS_ACCEPTED_CANCELED."""
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "AcceptedCanceled"
    response.filename = "log.tar.gz"
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
            response_grpc = await stub.GetLog(
                gateway_pb2.GetLogRequest(
                    cp_id="CP_001",
                    log_type=gateway_pb2.LOG_TYPE_SECURITY,
                    request_id=1,
                    location="https://x/",
                )
            )
        assert response_grpc.status == gateway_pb2.LOG_STATUS_ACCEPTED_CANCELED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_empty_location_returns_invalid_argument(
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
                await stub.GetLog(
                    gateway_pb2.GetLogRequest(
                        cp_id="CP_001",
                        log_type=gateway_pb2.LOG_TYPE_SECURITY,
                        request_id=1,
                        location="",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_log_unspecified_log_type_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """The proto field default is LOG_TYPE_UNSPECIFIED — a client
    forgetting to set it would otherwise silently default to
    DIAGNOSTICS. Reject at the boundary so the operator notices."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetLog(
                    gateway_pb2.GetLogRequest(
                        cp_id="CP_001",
                        # log_type left at default UNSPECIFIED
                        request_id=1,
                        location="https://x/",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- TC_074, TC_075_1, TC_075_2, TC_076 — certificate management ---------
#
# These tests need a real session_factory because the gRPC service
# writes to `charge_point_certificates` on Accepted. aiosqlite via
# the existing pattern (see test_ws_server_basic_auth.py).


def _make_pem(cn: str = "test-root", serial: int = 0xCAFE) -> str:
    """Build a self-signed PEM the gateway can parse + hash."""
    key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = _x509.Name([_x509.NameAttribute(_NameOID.COMMON_NAME, cn)])
    cert = (
        _x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(serial)
        .not_valid_before(_dt.datetime(2026, 1, 1))
        .not_valid_after(_dt.datetime(2027, 1, 1))
        .sign(key, _hashes.SHA256())
    )
    return cert.public_bytes(_ser.Encoding.PEM).decode()


@_pytest_asyncio.fixture
async def real_session_factory_with_cp() -> Any:
    """Real aiosqlite session factory pre-loaded with one
    `charge_points` row (cp_id='CP_001'). Used by the cert tests
    that exercise the persistence path."""
    from sqlalchemy import BigInteger as _BigInteger
    from sqlalchemy import Integer as _Integer

    # SQLite quirk: BigInteger PKs need an Integer variant for
    # autoincrement. Idempotent; metadata is module-global.
    for table in _Base.metadata.tables.values():
        for col in table.columns:
            if (
                col.primary_key
                and isinstance(col.type, _BigInteger)
                and "sqlite" not in getattr(col.type, "_variant_mapping", {})
            ):
                col.type = col.type.with_variant(_Integer(), "sqlite")  # type: ignore[assignment]

    engine = _create_async_engine(
        "sqlite+aiosqlite:///file:cert_mem?mode=memory&cache=shared&uri=true",
        future=True,
    )
    async with engine.begin() as conn:
        await conn.run_sync(_Base.metadata.create_all)
    factory = _amk(engine, expire_on_commit=False)
    async with factory() as session:
        session.add(_ChargePoint(cp_id="CP_001"))
        await session.commit()
    yield factory
    await engine.dispose()


@pytest.mark.asyncio
async def test_install_certificate_accepted_mirrors_to_db(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """The load-bearing test: charger Accepted → row written to
    `charge_point_certificates`. Operator UI relies on it."""
    pem = _make_pem(cn="csms-root", serial=0xC0FFEE)

    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.InstallCertificate(
                gateway_pb2.InstallCertificateRequest(
                    cp_id="CP_001",
                    certificate_type=gateway_pb2.CERTIFICATE_USE_CENTRAL_SYSTEM_ROOT,
                    pem=pem,
                )
            )
        assert response_grpc.status == gateway_pb2.CERTIFICATE_INSTALL_STATUS_ACCEPTED
        assert len(response_grpc.sha256_hash) == 64

        # The mirror row must exist with our cert.
        from sqlalchemy import select

        from eveys_ocpp.persistence.models import ChargePointCertificate

        async with real_session_factory_with_cp() as session:
            result = await session.execute(
                select(ChargePointCertificate).where(
                    ChargePointCertificate.sha256_hash == response_grpc.sha256_hash
                )
            )
            row = result.scalar_one_or_none()
        assert row is not None, "Accepted InstallCertificate must mirror to DB"
        assert row.certificate_type == "CentralSystemRootCertificate"
        assert row.pem == pem
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_install_certificate_rejected_does_not_mirror(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """Rejected → mirror row NOT written. Recording a cert the
    charger refused would lie to the operator UI."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Rejected"
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.InstallCertificate(
                gateway_pb2.InstallCertificateRequest(
                    cp_id="CP_001",
                    certificate_type=gateway_pb2.CERTIFICATE_USE_MANUFACTURER_ROOT,
                    pem=pem,
                )
            )
        assert response_grpc.status == gateway_pb2.CERTIFICATE_INSTALL_STATUS_REJECTED
        assert response_grpc.sha256_hash == ""

        from sqlalchemy import select

        from eveys_ocpp.persistence.models import ChargePointCertificate

        async with real_session_factory_with_cp() as session:
            result = await session.execute(select(ChargePointCertificate))
            assert result.scalars().all() == []
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_install_certificate_manufacturer_type_uses_correct_enum(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """proto MANUFACTURER_ROOT must map to ocpp_enums.CertificateUse.
    manufacturer_root_certificate, never silently re-route as CSMS
    root. This is the value-added correctness invariant."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock()
    response.status = "Accepted"
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            await stub.InstallCertificate(
                gateway_pb2.InstallCertificateRequest(
                    cp_id="CP_001",
                    certificate_type=gateway_pb2.CERTIFICATE_USE_MANUFACTURER_ROOT,
                    pem=pem,
                )
            )
        from ocpp.v16 import enums as ocpp_enums

        sent = cp.call.await_args.args[0]
        assert sent.certificate_type == ocpp_enums.CertificateUse.manufacturer_root_certificate
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_install_certificate_invalid_pem_returns_invalid_argument(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """A malformed PEM is caller error — reject at the boundary
    BEFORE dispatching to the charger. Avoid sending known-bad
    payloads."""
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.InstallCertificate(
                    gateway_pb2.InstallCertificateRequest(
                        cp_id="CP_001",
                        certificate_type=gateway_pb2.CERTIFICATE_USE_CENTRAL_SYSTEM_ROOT,
                        pem=(
                            "-----BEGIN CERTIFICATE-----\n"
                            "not-real-base64\n"
                            "-----END CERTIFICATE-----"
                        ),
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
        # Charger never saw a malformed cert.
        cp.call.assert_not_awaited()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_install_certificate_unspecified_type_returns_invalid_argument(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """proto default UNSPECIFIED is rejected at the boundary so
    operators don't accidentally rely on a silent default."""
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.InstallCertificate(
                    gateway_pb2.InstallCertificateRequest(
                        cp_id="CP_001",
                        # certificate_type left at UNSPECIFIED
                        pem=_make_pem(),
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_delete_certificate_round_trip(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """Full lifecycle: install → delete by hash. The §5.1 hash_data
    Dict is rebuilt from the stored PEM at delete time; the operator
    only knows the user-facing SHA-256."""
    pem = _make_pem(cn="root-to-delete", serial=0xBEEF)

    cp = MagicMock()
    cp.id = "CP_001"
    install_response = MagicMock(status="Accepted")
    delete_response = MagicMock(status="Accepted")
    # First call (Install) returns Accepted, second (Delete) returns
    # Accepted. Use side_effect to sequence them.
    cp.call = AsyncMock(side_effect=[install_response, delete_response])
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            install_grpc = await stub.InstallCertificate(
                gateway_pb2.InstallCertificateRequest(
                    cp_id="CP_001",
                    certificate_type=gateway_pb2.CERTIFICATE_USE_CENTRAL_SYSTEM_ROOT,
                    pem=pem,
                )
            )
            sha = install_grpc.sha256_hash
            delete_grpc = await stub.DeleteCertificate(
                gateway_pb2.DeleteCertificateRequest(cp_id="CP_001", sha256_hash=sha)
            )
        assert delete_grpc.status == gateway_pb2.CERTIFICATE_DELETE_STATUS_ACCEPTED

        # Mirror row should be gone after Accepted delete.
        from sqlalchemy import select

        from eveys_ocpp.persistence.models import ChargePointCertificate

        async with real_session_factory_with_cp() as session:
            result = await session.execute(select(ChargePointCertificate))
            assert result.scalars().all() == []

        # And the §5.1 hash_data Dict the gateway built must have all
        # 4 keys — verify by inspecting the OCPP request the charger
        # received.
        delete_call_args = cp.call.await_args_list[1].args[0]
        assert set(delete_call_args.certificate_hash_data.keys()) == {
            "hashAlgorithm",
            "issuerNameHash",
            "issuerKeyHash",
            "serialNumber",
        }
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_delete_certificate_unknown_hash_returns_not_found(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """Operator passes a hash we never recorded → NOT_FOUND. The
    charger is never dispatched (we'd have nothing to send)."""
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.DeleteCertificate(
                    gateway_pb2.DeleteCertificateRequest(cp_id="CP_001", sha256_hash="0" * 64)
                )
        assert exc.value.status == Status.NOT_FOUND
        cp.call.assert_not_awaited()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_certificate_signed_forwards_chain_verbatim(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    """CertificateSigned just transports — no parsing, no
    persistence. Pin that the chain reaches the charger unchanged."""
    chain = "-----BEGIN CERTIFICATE-----\nMIIB...\n-----END CERTIFICATE-----"
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock(status="Accepted")
    cp.call = AsyncMock(return_value=response)
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response_grpc = await stub.CertificateSigned(
                gateway_pb2.CertificateSignedRequest(cp_id="CP_001", certificate_chain=chain)
            )
        assert response_grpc.status == gateway_pb2.CERTIFICATE_SIGNED_STATUS_ACCEPTED
        sent = cp.call.await_args.args[0]
        assert sent.certificate_chain == chain
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_certificate_signed_empty_chain_returns_invalid_argument(
    real_session_factory_with_cp: Any, settings: Settings
) -> None:
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=real_session_factory_with_cp,
        settings=settings,
        connections=cm,
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.CertificateSigned(
                    gateway_pb2.CertificateSignedRequest(cp_id="CP_001", certificate_chain="")
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- TC_080, TC_081 — SignedUpdateFirmware -------------------------------
#
# Same fixture pattern as the cert-mgmt tests above. _make_pem
# already defined; reuse it for the signing certificate field.


@pytest.mark.asyncio
async def test_signed_update_firmware_accepted_round_trip(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Happy path: charger replies Accepted; gateway maps the OCPP
    enum to the proto LOG_STATUS_ACCEPTED. The §4.4 firmware Dict
    must include all 4 required spec keys."""
    pem = _make_pem(cn="firmware-signer", serial=0xF00D)
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock(status="Accepted")
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
            response_grpc = await stub.SignedUpdateFirmware(
                gateway_pb2.SignedUpdateFirmwareRequest(
                    cp_id="CP_001",
                    request_id=42,
                    location="https://fw.example/v2.bin",
                    retrieve_date_time="2026-05-09T00:00:00+00:00",
                    install_date_time="2026-05-09T03:00:00+00:00",
                    signing_certificate=pem,
                    signature="ZGVhZGJlZWY=",
                )
            )
        assert response_grpc.status == gateway_pb2.SIGNED_FIRMWARE_UPDATE_STATUS_ACCEPTED
        # The §4.4 firmware Dict the charger received must have the
        # spec's exact key names — a typo would silently fail the
        # spec validation on the charger side.
        sent = cp.call.await_args.args[0]
        assert sent.firmware["location"] == "https://fw.example/v2.bin"
        assert sent.firmware["retrieveDateTime"] == "2026-05-09T00:00:00+00:00"
        assert sent.firmware["installDateTime"] == "2026-05-09T03:00:00+00:00"
        assert sent.firmware["signingCertificate"] == pem
        assert sent.firmware["signature"] == "ZGVhZGJlZWY="
        assert sent.request_id == 42
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_signed_update_firmware_invalid_certificate_status_maps(
    fake_session_factory: Any, settings: Settings
) -> None:
    """TC_081 expectation: charger rejects with `InvalidCertificate`
    when the signing cert isn't trusted. Must round-trip to the
    proto enum so operator alerting sees the specific reason."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock(status="InvalidCertificate")
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
            response_grpc = await stub.SignedUpdateFirmware(
                gateway_pb2.SignedUpdateFirmwareRequest(
                    cp_id="CP_001",
                    request_id=1,
                    location="https://x/",
                    retrieve_date_time="2026-05-09T00:00:00+00:00",
                    signing_certificate=pem,
                    signature="QUJD",
                )
            )
        assert response_grpc.status == gateway_pb2.SIGNED_FIRMWARE_UPDATE_STATUS_INVALID_CERTIFICATE
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_signed_update_firmware_revoked_certificate_status_maps(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Charger CRL/OCSP check failed — operator misconfiguration
    (used a revoked cert). Must surface specifically, not collapse
    to generic Rejected."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock(status="RevokedCertificate")
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
            response_grpc = await stub.SignedUpdateFirmware(
                gateway_pb2.SignedUpdateFirmwareRequest(
                    cp_id="CP_001",
                    request_id=1,
                    location="https://x/",
                    retrieve_date_time="2026-05-09T00:00:00+00:00",
                    signing_certificate=pem,
                    signature="QUJD",
                )
            )
        assert response_grpc.status == gateway_pb2.SIGNED_FIRMWARE_UPDATE_STATUS_REVOKED_CERTIFICATE
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_signed_update_firmware_optional_install_date_time_omitted(
    fake_session_factory: Any, settings: Settings
) -> None:
    """`installDateTime` is optional per §4.4 — empty proto field
    means "install immediately after download". The Dict the charger
    receives must NOT contain the key when empty (some chargers
    treat presence-with-empty-value as a parse error)."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    response = MagicMock(status="Accepted")
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
            await stub.SignedUpdateFirmware(
                gateway_pb2.SignedUpdateFirmwareRequest(
                    cp_id="CP_001",
                    request_id=1,
                    location="https://x/",
                    retrieve_date_time="2026-05-09T00:00:00+00:00",
                    # install_date_time left empty
                    signing_certificate=pem,
                    signature="QUJD",
                )
            )
        sent = cp.call.await_args.args[0]
        assert "installDateTime" not in sent.firmware
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_signed_update_firmware_invalid_signing_cert_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Malformed signing-cert PEM is operator error — reject at the
    boundary BEFORE dispatching. The charger never sees a known-bad
    payload."""
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SignedUpdateFirmware(
                    gateway_pb2.SignedUpdateFirmwareRequest(
                        cp_id="CP_001",
                        request_id=1,
                        location="https://x/",
                        retrieve_date_time="2026-05-09T00:00:00+00:00",
                        signing_certificate=(
                            "-----BEGIN CERTIFICATE-----\n"
                            "not-real-base64\n"
                            "-----END CERTIFICATE-----"
                        ),
                        signature="QUJD",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
        cp.call.assert_not_awaited()
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_signed_update_firmware_empty_signature_returns_invalid_argument(
    fake_session_factory: Any, settings: Settings
) -> None:
    """An empty signature is meaningless — the charger has nothing
    to verify against. Reject at the boundary so operator notices."""
    pem = _make_pem()
    cp = MagicMock()
    cp.id = "CP_001"
    cp.call = AsyncMock()
    cm = ConnectionMap()
    cm.add(cp)
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.SignedUpdateFirmware(
                    gateway_pb2.SignedUpdateFirmwareRequest(
                        cp_id="CP_001",
                        request_id=1,
                        location="https://x/",
                        retrieve_date_time="2026-05-09T00:00:00+00:00",
                        signing_certificate=pem,
                        signature="",
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
        cp.call.assert_not_awaited()
    finally:
        server.close()
        await server.wait_closed()


# ---- ChangeAvailability (#180) ----------------------------------------------


@pytest.mark.asyncio
async def test_change_availability_accepted(fake_session_factory: Any, settings: Settings) -> None:
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ChangeAvailability(
                gateway_pb2.ChangeAvailabilityRequest(
                    cp_id="CP_001",
                    connector_id=1,
                    type=gateway_pb2.AVAILABILITY_TYPE_INOPERATIVE,
                )
            )
        assert response.status == gateway_pb2.CHANGE_AVAILABILITY_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_change_availability_scheduled(fake_session_factory: Any, settings: Settings) -> None:
    """Charger has a session in flight → replies Scheduled. Status
    must round-trip verbatim, not be re-mapped to Accepted (operators
    need to know the request will land later, not now)."""
    _, cm = _connected_cp("CP_001", "Scheduled")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ChangeAvailability(
                gateway_pb2.ChangeAvailabilityRequest(
                    cp_id="CP_001",
                    connector_id=2,
                    type=gateway_pb2.AVAILABILITY_TYPE_OPERATIVE,
                )
            )
        assert response.status == gateway_pb2.CHANGE_AVAILABILITY_STATUS_SCHEDULED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_change_availability_connector_zero_targets_whole_charger(
    fake_session_factory: Any, settings: Settings
) -> None:
    """`connector_id = 0` is the OCPP-specified way to target the whole
    charger; must NOT be rejected at the boundary the way UnlockConnector
    is. The two RPCs differ on this — the test pins the difference."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ChangeAvailability(
                gateway_pb2.ChangeAvailabilityRequest(
                    cp_id="CP_001",
                    connector_id=0,
                    type=gateway_pb2.AVAILABILITY_TYPE_INOPERATIVE,
                )
            )
        assert response.status == gateway_pb2.CHANGE_AVAILABILITY_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_change_availability_negative_connector_invalid(
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
                await stub.ChangeAvailability(
                    gateway_pb2.ChangeAvailabilityRequest(
                        cp_id="CP_001",
                        connector_id=-1,
                        type=gateway_pb2.AVAILABILITY_TYPE_INOPERATIVE,
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_change_availability_unspecified_type_invalid(
    fake_session_factory: Any, settings: Settings
) -> None:
    """`AVAILABILITY_TYPE_UNSPECIFIED` (the proto3 default for an unset
    enum field) is a client typo — must reject at the boundary so the
    charger doesn't see a no-op."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.ChangeAvailability(
                    gateway_pb2.ChangeAvailabilityRequest(
                        cp_id="CP_001",
                        connector_id=1,
                        # type left at AVAILABILITY_TYPE_UNSPECIFIED (proto default)
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()


# ---- ExtendedTriggerMessage (#182) -----------------------------------------


@pytest.mark.asyncio
async def test_extended_trigger_message_log_status_notification(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Headline use case for ExtendedTriggerMessage: prompt the charger
    to send a LogStatusNotification (the Whitepaper §4.7 addition that
    makes this RPC distinct from plain TriggerMessage)."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ExtendedTriggerMessage(
                gateway_pb2.ExtendedTriggerMessageRequest(
                    cp_id="CP_001",
                    requested_message=(
                        gateway_pb2.EXTENDED_TRIGGER_MESSAGE_TYPE_LOG_STATUS_NOTIFICATION
                    ),
                )
            )
        assert response.status == gateway_pb2.TRIGGER_MESSAGE_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_extended_trigger_message_sign_charge_point_certificate(
    fake_session_factory: Any, settings: Settings
) -> None:
    """The other Whitepaper-§4.7 addition. Pinned so a future refactor
    that drops it from the translator's mapping fails loud — operators
    on the cert-rotation path need both Whitepaper triggers."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ExtendedTriggerMessage(
                gateway_pb2.ExtendedTriggerMessageRequest(
                    cp_id="CP_001",
                    requested_message=(
                        gateway_pb2.EXTENDED_TRIGGER_MESSAGE_TYPE_SIGN_CHARGE_POINT_CERTIFICATE
                    ),
                )
            )
        assert response.status == gateway_pb2.TRIGGER_MESSAGE_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_extended_trigger_message_core_type_passes_through(
    fake_session_factory: Any, settings: Settings
) -> None:
    """The Extended variant accepts both old + new types (Whitepaper
    §4.7); a Core-profile BootNotification trigger sent through the
    Extended RPC must work the same as via plain TriggerMessage."""
    _, cm = _connected_cp("CP_001", "Accepted")
    service = OcppGatewayService(
        session_factory=fake_session_factory, settings=settings, connections=cm
    )
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ExtendedTriggerMessage(
                gateway_pb2.ExtendedTriggerMessageRequest(
                    cp_id="CP_001",
                    requested_message=gateway_pb2.EXTENDED_TRIGGER_MESSAGE_TYPE_BOOT_NOTIFICATION,
                )
            )
        assert response.status == gateway_pb2.TRIGGER_MESSAGE_STATUS_ACCEPTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_extended_trigger_message_unspecified_invalid(
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
                await stub.ExtendedTriggerMessage(
                    gateway_pb2.ExtendedTriggerMessageRequest(
                        cp_id="CP_001",
                        # requested_message left at UNSPECIFIED (proto default)
                    )
                )
        assert exc.value.status == Status.INVALID_ARGUMENT
    finally:
        server.close()
        await server.wait_closed()
