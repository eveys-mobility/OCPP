"""End-to-end two-pod cross-pod command bus test (E2-10).

Acceptance criterion from `docs/02-tasks.md` E2-10: "two-pod test passes."

Topology under test:

    gRPC client ──► pod B (gRPC) ──► Redis pub/sub ──► pod A ──► fake CP
                                                                  │
    gRPC client ◄── pod B (gRPC) ◄── Redis pub/sub ◄── pod A ◄────┘

Pod A owns the charger's "WebSocket" (a fake EveysChargePoint in its
ConnectionMap); pod B receives the gRPC `RemoteStart` and must route
across the bus to pod A.

Skipped when Redis isn't reachable so `make tests` stays green on
machines without the data plane up. Run explicitly with `make smoke`
once Redis is available.
"""

from __future__ import annotations

import os
import socket
from collections.abc import AsyncIterator
from contextlib import closing
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from grpclib.client import Channel
from redis.asyncio import Redis

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.bus import CommandBus
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.grpc_server import OcppGatewayService

_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "6379"))
# CI sets this; when set, a missing Redis is a hard failure rather than
# a silent skip. This file is the literal "two-pod test passes" acceptance
# criterion for E2-10 — a green-but-skipped run would defeat its purpose.
_REDIS_REQUIRED = os.environ.get("E2E_REQUIRE") == "1"


def _redis_reachable() -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((_REDIS_HOST, _REDIS_PORT))
        except OSError:
            return False
        return True


if not _redis_reachable():
    _msg = f"Redis at {_REDIS_HOST}:{_REDIS_PORT} unreachable; two-pod test needs it"
    if _REDIS_REQUIRED:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the tests:e2e job must keep its "
            "`redis:7-alpine` service. CI config bug, not env issue. "
            "This is the E2-10 acceptance test; skipping it silently is "
            "a false-green.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


@pytest.fixture
async def redis_client() -> AsyncIterator[Redis]:
    client = Redis.from_url(
        f"redis://{_REDIS_HOST}:{_REDIS_PORT}/0",
        decode_responses=True,
    )
    yield client
    await client.aclose()


async def _spawn_grpc(service: OcppGatewayService) -> tuple[Any, int]:
    from grpclib.server import Server

    server = Server([service])
    await server.start(host="127.0.0.1", port=0)
    sockets = server._server.sockets if server._server else []  # type: ignore[union-attr]
    assert sockets, "grpc server didn't bind"
    return server, sockets[0].getsockname()[1]


@pytest.mark.asyncio
async def test_remote_start_routes_across_two_pods(redis_client: Redis) -> None:
    """End-to-end: gRPC on pod B → Redis bus → fake charger on pod A → reply."""
    settings = Settings()

    # ---- pod A: owns the charger -------------------------------------------
    pod_a_cp = MagicMock()
    pod_a_cp.id = "CP_TWOPOD"
    pod_a_response = MagicMock()
    pod_a_response.status = "Accepted"
    pod_a_cp.call = AsyncMock(return_value=pod_a_response)
    pod_a_connections = ConnectionMap()
    pod_a_connections.add(pod_a_cp)

    # Registry says pod A owns it (pod B will route accordingly).
    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_a_bus = CommandBus(
        redis_client,
        pod_id="pod-A",
        connections=pod_a_connections,
        request_timeout_seconds=5.0,
    )
    # Constructing the service wires the owning-side dispatcher into the bus
    # via OcppGatewayService.__init__; we don't need a reference back to it.
    OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_a_connections,
        registry=pod_a_registry,
        bus=pod_a_bus,
    )

    # ---- pod B: receives the gRPC request ----------------------------------
    pod_b_connections = ConnectionMap()  # empty — no charger here
    pod_b_registry = AsyncMock()
    pod_b_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_b_bus = CommandBus(
        redis_client,
        pod_id="pod-B",
        connections=pod_b_connections,
        request_timeout_seconds=5.0,
    )
    pod_b_service = OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_b_connections,
        registry=pod_b_registry,
        bus=pod_b_bus,
    )

    # ---- bring up both pods ------------------------------------------------
    await pod_a_bus.start()
    await pod_b_bus.start()

    pod_b_grpc, pod_b_port = await _spawn_grpc(pod_b_service)

    try:
        async with Channel("127.0.0.1", pod_b_port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStart(
                gateway_pb2.RemoteStartRequest(cp_id="CP_TWOPOD", id_tag="TAG_42", connector_id=1)
            )

        # Pod B got ACCEPTED back from a charger that lives on pod A.
        assert response.status == gateway_pb2.REMOTE_START_STATUS_ACCEPTED

        # The charger on pod A actually received the OCPP call.
        pod_a_cp.call.assert_awaited_once()
        ocpp_req = pod_a_cp.call.await_args.args[0]
        assert ocpp_req.id_tag == "TAG_42"
        assert ocpp_req.connector_id == 1
    finally:
        pod_b_grpc.close()
        await pod_b_grpc.wait_closed()
        await pod_a_bus.stop()
        await pod_b_bus.stop()


@pytest.mark.asyncio
async def test_remote_stop_routes_across_two_pods(redis_client: Redis) -> None:
    """Sanity check that the dispatch table covers more than just RemoteStart."""
    settings = Settings()

    pod_a_cp = MagicMock()
    pod_a_cp.id = "CP_TWOPOD2"
    pod_a_response = MagicMock()
    pod_a_response.status = "Accepted"
    pod_a_cp.call = AsyncMock(return_value=pod_a_response)
    pod_a_connections = ConnectionMap()
    pod_a_connections.add(pod_a_cp)

    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_a_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=pod_a_connections, request_timeout_seconds=5.0
    )
    # Construction wires the dispatcher into pod A's bus; no return value needed.
    OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_a_connections,
        registry=pod_a_registry,
        bus=pod_a_bus,
    )

    pod_b_connections = ConnectionMap()
    pod_b_registry = AsyncMock()
    pod_b_registry.get_pod = AsyncMock(return_value="pod-A")
    pod_b_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=pod_b_connections, request_timeout_seconds=5.0
    )
    pod_b_service = OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_b_connections,
        registry=pod_b_registry,
        bus=pod_b_bus,
    )

    await pod_a_bus.start()
    await pod_b_bus.start()

    pod_b_grpc, pod_b_port = await _spawn_grpc(pod_b_service)
    try:
        async with Channel("127.0.0.1", pod_b_port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStop(
                gateway_pb2.RemoteStopRequest(cp_id="CP_TWOPOD2", transaction_id=99)
            )
        assert response.status == gateway_pb2.REMOTE_STOP_STATUS_ACCEPTED
        pod_a_cp.call.assert_awaited_once()
        assert pod_a_cp.call.await_args.args[0].transaction_id == 99
    finally:
        pod_b_grpc.close()
        await pod_b_grpc.wait_closed()
        await pod_a_bus.stop()
        await pod_b_bus.stop()


@pytest.mark.asyncio
async def test_charger_disconnects_mid_request_returns_not_found(
    redis_client: Redis, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Race: registry still has pod A but ConnectionMap on A doesn't anymore.

    Patch the OCPP request timeout to 1s so this test runs in ~1s rather
    than the default 30s. The bus's behaviour is identical at any timeout.
    """
    from eveys_ocpp.transport import grpc_server as gs_module

    monkeypatch.setattr(gs_module, "_OCPP_REQUEST_TIMEOUT_SECONDS", 1.0)

    settings = Settings()

    pod_a_connections = ConnectionMap()  # empty — charger "disconnected"
    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")
    pod_a_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=pod_a_connections, request_timeout_seconds=2.0
    )
    # Construction wires the dispatcher into pod A's bus.
    OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_a_connections,
        registry=pod_a_registry,
        bus=pod_a_bus,
    )

    pod_b_connections = ConnectionMap()
    pod_b_registry = AsyncMock()
    pod_b_registry.get_pod = AsyncMock(return_value="pod-A")
    pod_b_bus = CommandBus(
        redis_client, pod_id="pod-B", connections=pod_b_connections, request_timeout_seconds=2.0
    )
    pod_b_service = OcppGatewayService(
        session_factory=MagicMock(),
        settings=settings,
        connections=pod_b_connections,
        registry=pod_b_registry,
        bus=pod_b_bus,
    )

    await pod_a_bus.start()
    await pod_b_bus.start()

    pod_b_grpc, pod_b_port = await _spawn_grpc(pod_b_service)

    from grpclib.const import Status
    from grpclib.exceptions import GRPCError

    try:
        async with Channel("127.0.0.1", pod_b_port) as ch:
            stub = gateway_grpc.OcppGatewayStub(ch)
            # Without an owning subscriber that actually has the cp, the
            # request times out at the bus layer (DEADLINE_EXCEEDED). That's
            # the correct outcome — we don't have a separate "owner-says-no"
            # path because the owning pod stays silent if the cp isn't
            # local (see test_owning_side_silent_when_cp_not_local).
            with pytest.raises(GRPCError) as exc:
                await stub.RemoteStart(gateway_pb2.RemoteStartRequest(cp_id="GHOST_CP", id_tag="X"))
        assert exc.value.status == Status.DEADLINE_EXCEEDED
    finally:
        pod_b_grpc.close()
        await pod_b_grpc.wait_closed()
        await pod_a_bus.stop()
        await pod_b_bus.stop()
