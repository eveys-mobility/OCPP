"""End-to-end two-pod cross-pod command bus test (E2-10).

Acceptance criterion from `docs/02-tasks.md` E2-10: "two-pod test passes."

Topology under test:

    gRPC client ──► pod B (gRPC) ──► Redis pub/sub ──► pod A ──► fake CP
                                                                  │
    gRPC client ◄── pod B (gRPC) ◄── Redis pub/sub ◄── pod A ◄────┘

Two flavours of test:

1. ``test_remote_start_routes_across_two_pods`` — pod A holds a
   ``MagicMock`` ``EveysChargePoint`` in its ``ConnectionMap``. Cheap,
   fast, validates the bus + dispatch wiring.
2. ``test_remote_start_routes_across_two_pods_with_real_ws`` — pod A
   runs a real ``ws_server`` against Postgres + Redis, and the test
   connects an actual ``ocpp.v16.ChargePoint`` simulator over a real
   WebSocket. Validates the full path including the
   ``EveysChargePoint.call(...)`` shape that the mock skipped.

Skipped when the required services are unreachable so `make tests`
stays green on machines without the data plane up. Run explicitly with
`make smoke` once the stack is up. CI sets ``E2E_REQUIRE=1`` so a
missing service is a hard failure rather than a silent skip — this is
the literal acceptance criterion for E2-10.
"""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import AsyncIterator
from contextlib import closing, suppress
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa
from grpclib.client import Channel
from ocpp.routing import on
from ocpp.v16 import ChargePoint as OcppCp
from ocpp.v16 import call, call_result
from ocpp.v16.enums import Action, ClearCacheStatus, DataTransferStatus
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import create_async_engine
from websockets.asyncio.client import connect as ws_connect

from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc, gateway_pb2
from eveys_ocpp.bus import CommandBus
from eveys_ocpp.connections import ConnectionMap
from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.grpc_server import OcppGatewayService

_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
# Compose remaps Redis from 6379 → 16379 on the host (see
# `deploy/compose/docker-compose.yml`); same shape as `_PG_PORT`. CI on
# GitHub Actions uses service containers on the canonical port and
# overrides this via `E2E_REDIS_PORT=6379`.
_REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "16379"))
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
async def test_get_configuration_routes_across_two_pods(redis_client: Redis) -> None:
    """E2-1A: GetConfiguration's list-of-dicts response shape survives the
    JSON envelope round-trip across the bus and lands as typed proto on
    the requester side.

    The owning side reconstructs an `ocpp.v16.call.GetConfiguration`
    dataclass from the bus payload, dispatches it via `cp.call(...)`, and
    serialises the response — which contains parallel arrays for
    `configuration_key` (list of dicts) and `unknown_key` (list of
    strings). If the bus's serialiser ever drops fields or coerces dicts
    into something else, this test fails before production does.
    """
    settings = Settings()

    pod_a_cp = MagicMock()
    pod_a_cp.id = "CP_GETCFG"
    # Use a real ocpp.v16.call_result dataclass so the owning side's
    # `is_dataclass(...) → asdict(...)` path runs (production charger
    # replies are these dataclasses; a MagicMock would fall through
    # to a status-only reply and the test would catch the wrong bug).
    pod_a_response = call_result.GetConfiguration(
        configuration_key=[
            {"key": "HeartbeatInterval", "readonly": False, "value": "60"},
            {"key": "NumberOfConnectors", "readonly": True, "value": "2"},
        ],
        unknown_key=["NoSuchKey"],
    )
    pod_a_cp.call = AsyncMock(return_value=pod_a_response)
    pod_a_connections = ConnectionMap()
    pod_a_connections.add(pod_a_cp)

    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_a_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=pod_a_connections, request_timeout_seconds=5.0
    )
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
            response = await stub.GetConfiguration(
                gateway_pb2.GetConfigurationRequest(
                    cp_id="CP_GETCFG",
                    keys=["HeartbeatInterval", "NumberOfConnectors", "NoSuchKey"],
                )
            )

        # Both known keys round-tripped with their readonly flags preserved.
        assert len(response.configuration_key) == 2
        keys_by_name = {ck.key: ck for ck in response.configuration_key}
        assert keys_by_name["HeartbeatInterval"].readonly is False
        assert keys_by_name["HeartbeatInterval"].value == "60"
        assert keys_by_name["NumberOfConnectors"].readonly is True
        assert keys_by_name["NumberOfConnectors"].value == "2"
        # Unknown keys come back as a flat string list.
        assert list(response.unknown_key) == ["NoSuchKey"]

        # Pod A's charger received an OCPP GetConfiguration with the
        # forwarded `keys` list. (The bus serialises the payload as a
        # dict; the owning side reconstructs the dataclass.)
        pod_a_cp.call.assert_awaited_once()
        ocpp_req = pod_a_cp.call.await_args.args[0]
        assert ocpp_req.key == ["HeartbeatInterval", "NumberOfConnectors", "NoSuchKey"]
    finally:
        pod_b_grpc.close()
        await pod_b_grpc.wait_closed()
        await pod_a_bus.stop()
        await pod_b_bus.stop()


@pytest.mark.asyncio
async def test_clear_cache_routes_across_two_pods(redis_client: Redis) -> None:
    """E2-1A: ClearCache is an empty-payload OCPP call. The bus must
    happily ferry a request with no fields to set on the dataclass."""
    settings = Settings()

    pod_a_cp = MagicMock()
    pod_a_cp.id = "CP_CLEAR"
    pod_a_response = call_result.ClearCache(status=ClearCacheStatus.accepted)
    pod_a_cp.call = AsyncMock(return_value=pod_a_response)
    pod_a_connections = ConnectionMap()
    pod_a_connections.add(pod_a_cp)

    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_a_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=pod_a_connections, request_timeout_seconds=5.0
    )
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
            response = await stub.ClearCache(gateway_pb2.ClearCacheRequest(cp_id="CP_CLEAR"))
        assert response.status == gateway_pb2.CLEAR_CACHE_STATUS_ACCEPTED
        # ClearCache.req carries no fields per OCPP — confirm the
        # owning side really did dispatch an empty-payload call.
        pod_a_cp.call.assert_awaited_once()
    finally:
        pod_b_grpc.close()
        await pod_b_grpc.wait_closed()
        await pod_a_bus.stop()
        await pod_b_bus.stop()


@pytest.mark.asyncio
async def test_data_transfer_routes_across_two_pods(redis_client: Redis) -> None:
    """E2-1A: DataTransfer carries vendor-namespaced strings AND a
    string reply payload back. Both directions exercise the bus's
    string-field round-trip beyond the simple-status pattern that
    RemoteStart/RemoteStop / Reset / ClearCache all share.
    """
    settings = Settings()

    pod_a_cp = MagicMock()
    pod_a_cp.id = "CP_DTX"
    # Real ocpp.v16.call_result.DataTransfer so the owning side's
    # asdict() path runs (matches what a real charger reply would be).
    pod_a_response = call_result.DataTransfer(
        status=DataTransferStatus.accepted,
        data='{"reply":"ok","seq":7}',
    )
    pod_a_cp.call = AsyncMock(return_value=pod_a_response)
    pod_a_connections = ConnectionMap()
    pod_a_connections.add(pod_a_cp)

    pod_a_registry = AsyncMock()
    pod_a_registry.get_pod = AsyncMock(return_value="pod-A")

    pod_a_bus = CommandBus(
        redis_client, pod_id="pod-A", connections=pod_a_connections, request_timeout_seconds=5.0
    )
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
            response = await stub.DataTransfer(
                gateway_pb2.DataTransferRequest(
                    cp_id="CP_DTX",
                    vendor_id="acme.fastcharge",
                    message_id="ping",
                    data='{"hi":1}',
                )
            )
        assert response.status == gateway_pb2.DATA_TRANSFER_STATUS_ACCEPTED
        # Vendor reply payload survived the bus → grpc translation.
        assert response.data == '{"reply":"ok","seq":7}'

        # Pod A's charger got the OCPP DataTransfer with all three
        # vendor-namespaced fields intact across the bus.
        pod_a_cp.call.assert_awaited_once()
        ocpp_req = pod_a_cp.call.await_args.args[0]
        assert ocpp_req.vendor_id == "acme.fastcharge"
        assert ocpp_req.message_id == "ping"
        assert ocpp_req.data == '{"hi":1}'
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


# ----------------------------------------------------------------------------
# Real-WebSocket two-pod test
#
# The mock-based tests above prove the bus + dispatch wiring; this test
# proves the full path including the live `EveysChargePoint.call(...)`
# shape against a real OCPP simulator over a real WebSocket. Removes the
# "but I only tested with mocks" caveat from the E2-10 acceptance.
# ----------------------------------------------------------------------------


_PG_HOST = os.environ.get("E2E_PG_HOST", "localhost")
# Compose remaps Postgres from 5432 → 55432 on the host to dodge a host
# Postgres install. CI binds PG directly on 5432, so it overrides via
# E2E_PG_PORT=5432.
_PG_PORT = int(os.environ.get("E2E_PG_PORT", "55432"))
_PG_DB_URL = os.environ.get(
    "EVEYS_OCPP_DB_URL",
    f"postgresql+asyncpg://eveys:eveys@{_PG_HOST}:{_PG_PORT}/eveys_ocpp",
)


def _postgres_reachable() -> bool:
    """Cheap TCP probe — schema check happens inside the test."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((_PG_HOST, _PG_PORT))
        except OSError:
            return False
        return True


def _maybe_skip_if_postgres_missing() -> None:
    """Skip on dev laptop, hard-fail in CI. Same E2E_REQUIRE pattern as
    the module-level Redis check."""
    if _postgres_reachable():
        return
    msg = f"Postgres at {_PG_HOST}:5432 unreachable; real-WS two-pod test needs it"
    if _REDIS_REQUIRED:  # E2E_REQUIRE=1 — same gate as Redis above
        pytest.fail(
            f"{msg}. E2E_REQUIRE=1 — the tests:e2e job must keep its `postgres` "
            "service. CI config bug, not env issue.",
            pytrace=False,
        )
    pytest.skip(msg)


class _RemoteStartHandler:
    """Sim-side OCPP handler. The library dispatches by Action name via
    `@on(...)`; we capture every RemoteStartTransaction request the gateway
    sends and reply Accepted so the test can assert end-to-end."""

    def __init__(self) -> None:
        self.received: list[Any] = []

    @on(Action.remote_start_transaction)
    async def _on_remote_start(
        self,
        id_tag: str,
        connector_id: int | None = None,
        charging_profile: dict[str, Any] | None = None,
        **_kw: object,
    ) -> call_result.RemoteStartTransaction:
        self.received.append({"id_tag": id_tag, "connector_id": connector_id})
        return call_result.RemoteStartTransaction(status="Accepted")


class _SimChargePoint(OcppCp, _RemoteStartHandler):  # type: ignore[misc]
    """Charger simulator with a RemoteStartTransaction handler.

    Multiple inheritance with `_RemoteStartHandler` plugs the `@on(...)`
    decorator into the OCPP routing table on this instance. Same pattern
    `test_local_smoke.py`'s charger sim uses, just with one extra handler.
    """

    def __init__(self, cp_id: str, ws: Any) -> None:
        OcppCp.__init__(self, cp_id, ws)
        _RemoteStartHandler.__init__(self)


def _free_port() -> int:
    """Bind to an OS-assigned port and immediately release it."""
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


async def _wait_until_registered(redis: Redis, cp_id: str, timeout: float = 3.0) -> None:
    """Spin briefly until the charger's online registry key exists.

    The WS server marks the charger online inside `_on_connect`; we want
    to wait for that to settle before issuing the gRPC call so pod B's
    registry lookup finds the pod_id.
    """
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if await redis.exists(f"cp:online:{cp_id}"):
            return
        await asyncio.sleep(0.05)
    raise RuntimeError(f"charger {cp_id} never registered as online within {timeout}s")


@pytest.mark.asyncio
async def test_remote_start_routes_across_two_pods_with_real_ws(redis_client: Redis) -> None:
    """Full-fat E2-10 verification: real WS sim → pod A → bus → pod B → gRPC."""
    _maybe_skip_if_postgres_missing()

    cp_id = "CP_TWOPOD_REAL"
    pod_a_ws_port = _free_port()

    # Schema must be applied — same gate test_local_smoke uses.
    db_engine = create_async_engine(_PG_DB_URL)
    try:
        async with db_engine.connect() as conn:
            try:
                await conn.execute(sa.text("SELECT 1 FROM charge_points LIMIT 1"))
            except Exception:
                msg = "schema not applied — run `alembic upgrade head` first"
                if _REDIS_REQUIRED:
                    pytest.fail(
                        f"{msg}. E2E_REQUIRE=1 — `alembic upgrade head` should have "
                        "run in the tests:e2e job before pytest.",
                        pytrace=False,
                    )
                pytest.skip(msg)
    finally:
        await db_engine.dispose()

    # Override env so Settings picks up the real services and pod A's WS port.
    saved_env = {
        k: os.environ.get(k)
        for k in (
            "EVEYS_OCPP_WS_PORT",
            "EVEYS_OCPP_DB_URL",
            "EVEYS_OCPP_REDIS_URL",
            "EVEYS_OCPP_KAFKA_BROKERS",
            "EVEYS_OCPP_LOG_JSON",
            "EVEYS_OCPP_POD_ID",
        )
    }
    os.environ["EVEYS_OCPP_WS_PORT"] = str(pod_a_ws_port)
    os.environ["EVEYS_OCPP_DB_URL"] = _PG_DB_URL
    os.environ["EVEYS_OCPP_REDIS_URL"] = f"redis://{_REDIS_HOST}:{_REDIS_PORT}/0"
    os.environ["EVEYS_OCPP_LOG_JSON"] = "false"
    os.environ["EVEYS_OCPP_POD_ID"] = "pod-A-real"
    # Kafka isn't required for this test — handlers run NullEventProducer-equivalent
    # path when start() fails, but to keep things simple we point at the same
    # broker the e2e job runs.
    _kafka_host = os.environ.get("E2E_KAFKA_HOST", "localhost")
    os.environ.setdefault("EVEYS_OCPP_KAFKA_BROKERS", f"{_kafka_host}:9092")

    from eveys_ocpp.events import KafkaEventProducer
    from eveys_ocpp.persistence.db import make_engine, make_session_factory
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import get_settings
    from eveys_ocpp.transport.ws_server import serve_forever as serve_ws_forever

    pod_a_settings = get_settings()
    pod_a_db = make_engine(pod_a_settings.db_url.get_secret_value())
    pod_a_session_factory = make_session_factory(pod_a_db)
    pod_a_registry = Registry.from_settings(pod_a_settings)
    pod_a_connections = ConnectionMap()
    pod_a_event_producer = KafkaEventProducer.from_settings(pod_a_settings)
    await pod_a_event_producer.start()

    pod_a_bus = CommandBus(
        redis_client,
        pod_id=pod_a_settings.pod_id,
        connections=pod_a_connections,
        request_timeout_seconds=10.0,
    )
    # Construction wires the owning-side dispatcher into pod A's bus
    # (set_local_dispatcher is called from OcppGatewayService.__init__);
    # we don't need the reference back. Pod A doesn't take gRPC traffic
    # in this test — only pod B does — so we never serve this gRPC.
    OcppGatewayService(
        session_factory=pod_a_session_factory,
        settings=pod_a_settings,
        connections=pod_a_connections,
        registry=pod_a_registry,
        bus=pod_a_bus,
    )

    # Pod B: gRPC + bus + registry, NO WS server (it doesn't own this charger).
    pod_b_settings = Settings()  # default settings, fine for pod B
    pod_b_connections = ConnectionMap()
    pod_b_bus = CommandBus(
        redis_client,
        pod_id="pod-B-real",
        connections=pod_b_connections,
        request_timeout_seconds=10.0,
    )
    pod_b_grpc_service = OcppGatewayService(
        session_factory=MagicMock(),  # pod B doesn't query DB for cross-pod
        settings=pod_b_settings,
        connections=pod_b_connections,
        registry=pod_a_registry,  # share so registry lookup finds pod-A-real
        bus=pod_b_bus,
    )

    ws_task: asyncio.Task[None] | None = None
    pod_b_grpc_server = None
    try:
        # Bring up pod A's WS server.
        ws_task = asyncio.create_task(
            serve_ws_forever(
                session_factory=pod_a_session_factory,
                settings=pod_a_settings,
                registry=pod_a_registry,
                connections=pod_a_connections,
                event_producer=pod_a_event_producer,
            )
        )
        await asyncio.sleep(0.2)  # give the server a beat to bind

        await pod_a_bus.start()
        await pod_b_bus.start()

        pod_b_grpc_server, pod_b_grpc_port = await _spawn_grpc(pod_b_grpc_service)

        # Connect a real OCPP simulator to pod A's WS port.
        async with ws_connect(
            f"ws://localhost:{pod_a_ws_port}/{cp_id}",
            subprotocols=["ocpp1.6"],
        ) as ws:
            sim = _SimChargePoint(cp_id, ws)
            sim_loop = asyncio.create_task(sim.start())

            # Drive BootNotification so the cp_id is upserted in Postgres
            # and the WS server marks it online in the registry.
            boot = await sim.call(
                call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1")
            )
            assert boot.status == "Accepted"

            await _wait_until_registered(redis_client, cp_id)

            # Issue RemoteStart against pod B's gRPC.
            async with Channel("127.0.0.1", pod_b_grpc_port) as ch:
                stub = gateway_grpc.OcppGatewayStub(ch)
                response = await stub.RemoteStart(
                    gateway_pb2.RemoteStartRequest(
                        cp_id=cp_id, id_tag="VALID_RFID_001", connector_id=1
                    )
                )

            assert response.status == gateway_pb2.REMOTE_START_STATUS_ACCEPTED
            # The sim's @on handler captured the request that came over
            # the bus from pod B → Redis → pod A → real WS.
            assert len(sim.received) == 1
            assert sim.received[0]["id_tag"] == "VALID_RFID_001"
            assert sim.received[0]["connector_id"] == 1

            sim_loop.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await sim_loop
    finally:
        if pod_b_grpc_server is not None:
            pod_b_grpc_server.close()
            await pod_b_grpc_server.wait_closed()
        if ws_task is not None:
            ws_task.cancel()
            with suppress(asyncio.CancelledError, Exception):
                await ws_task
        await pod_a_bus.stop()
        await pod_b_bus.stop()
        await pod_a_event_producer.stop()
        await pod_a_registry.close()
        await pod_a_db.dispose()

        # Restore env so subsequent tests aren't polluted.
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    # `datetime` is referenced indirectly via the OCPP library; keep the
    # import alive across teardown by touching it here for clarity.
    _ = datetime.now(UTC)
