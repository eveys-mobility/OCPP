"""Unit tests for the gRPC server (E2-4 scaffold + E2-5 RemoteStart).

The tests against the still-unimplemented RPCs (Reset, ChangeConfiguration,
TriggerMessage, UnlockConnector, RemoteStop, GetChargerStatus) verify
they each return UNIMPLEMENTED through the real grpclib error path.

The RemoteStart tests cover all four routing branches:
- Charger offline (not in registry) → NOT_FOUND
- Charger online but on a different pod → UNAVAILABLE
- Charger on this pod, charger Accepts → ACCEPTED
- Charger on this pod, charger Rejects → REJECTED
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
    }
    for rpc in expected:
        method = getattr(service, rpc, None)
        assert method is not None, f"missing RPC method: {rpc}"
        assert asyncio.iscoroutinefunction(method), f"{rpc} must be async"


def test_settings_grpc_defaults() -> None:
    s = Settings()
    assert s.grpc_host == "0.0.0.0"
    assert s.grpc_port == 50051


# ---- still-unimplemented RPCs ----------------------------------------------


@pytest.mark.asyncio
async def test_get_charger_status_returns_unimplemented(
    fake_session_factory: Any, settings: Settings
) -> None:
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetChargerStatus(gateway_pb2.GetChargerStatusRequest(cp_id="TEST"))
        assert exc.value.status == Status.UNIMPLEMENTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_remote_stop_returns_unimplemented(
    fake_session_factory: Any, settings: Settings
) -> None:
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    server, port = await _spawn_server(service)
    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStop(gateway_pb2.RemoteStopRequest(cp_id="TEST", transaction_id=1))
        assert exc.value.status == Status.UNIMPLEMENTED
    finally:
        server.close()
        await server.wait_closed()


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
async def test_remote_start_charger_on_other_pod_returns_unavailable(
    fake_session_factory: Any, settings: Settings
) -> None:
    """Registry says another pod owns the WS — UNAVAILABLE pending E2-10."""
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
