"""Unit tests for the gRPC server scaffold (E2-4).

These tests don't need a real backend — they verify:
- Generated stubs load.
- The server class exposes every RPC defined in the proto.
- Each RPC raises UNIMPLEMENTED through the grpclib error path.
- Server start/stop is clean.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest
from grpclib.client import Channel
from grpclib.const import Status
from grpclib.exceptions import GRPCError
from grpclib.server import Server

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.grpc_server import OcppGatewayService


@pytest.fixture
def fake_session_factory() -> Any:
    return MagicMock()


@pytest.fixture
def settings() -> Settings:
    return Settings()


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


@pytest.mark.asyncio
async def test_remote_start_returns_unimplemented(
    fake_session_factory: Any, settings: Settings
) -> None:
    """End-to-end through a real grpclib server on a loopback socket.

    Spawns the server bound to 127.0.0.1 on an OS-assigned port; opens
    a channel; calls RemoteStart; expects UNIMPLEMENTED.
    """
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    server = Server([service])
    await server.start(host="127.0.0.1", port=0)
    # OS-assigned port discoverable via the underlying socket.
    sockets = server._server.sockets if server._server else []  # type: ignore[union-attr]
    assert sockets, "server didn't bind"
    port = sockets[0].getsockname()[1]

    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="TEST", id_tag="VALID"))
        assert exc.value.status == Status.UNIMPLEMENTED
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_get_charger_status_returns_unimplemented(
    fake_session_factory: Any, settings: Settings
) -> None:
    service = OcppGatewayService(session_factory=fake_session_factory, settings=settings)
    server = Server([service])
    await server.start(host="127.0.0.1", port=0)
    sockets = server._server.sockets if server._server else []  # type: ignore[union-attr]
    port = sockets[0].getsockname()[1]

    try:
        async with Channel("127.0.0.1", port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            with pytest.raises(GRPCError) as exc:
                await stub.GetChargerStatus(gateway_pb2.GetChargerStatusRequest(cp_id="TEST"))
        assert exc.value.status == Status.UNIMPLEMENTED
    finally:
        server.close()
        await server.wait_closed()


def test_settings_grpc_defaults() -> None:
    s = Settings()
    assert s.grpc_host == "0.0.0.0"
    assert s.grpc_port == 50051
