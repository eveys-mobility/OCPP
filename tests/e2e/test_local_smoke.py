"""End-to-end smoke test against `make compose-up` data plane.

Skipped when Postgres / Redis / Kafka / ClickHouse aren't reachable —
this lets `make tests` stay green on machines without the stack running.
Run explicitly with `make smoke` once the stack is up.

Two layers of testing in this file:

1. Reachability — the four data-plane services accept TCP connections and
   ClickHouse responds to `/ping`. Cheap; runs in milliseconds.
2. Charger flow (E1-13) — a charger simulator connects to a locally-spawned
   `eveys/ocpp` service, drives BootNotification → Authorize →
   StartTransaction → StopTransaction, and asserts the resulting DB rows.
   Requires Alembic schema applied (`alembic upgrade head`).
"""

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import AsyncIterator, Iterator
from contextlib import closing, suppress
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from ocpp.v16 import ChargePoint as Cp
from ocpp.v16 import call
from sqlalchemy.ext.asyncio import create_async_engine
from websockets.asyncio.client import connect

# Data-plane hosts. Default to `localhost` (`make compose-up` on a dev
# workstation). In GitLab CI, where the stack runs as `services:` sidecars, the
# pipeline overrides these via env to the service aliases (e.g. `postgres`).
_PG_HOST = os.environ.get("E2E_PG_HOST", "localhost")
# Compose remaps Postgres from 5432 → 55432 on the host to dodge a host
# Postgres install. CI binds PG directly on 5432, so it overrides via
# E2E_PG_PORT=5432.
_PG_PORT = int(os.environ.get("E2E_PG_PORT", "55432"))
_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
_REDIS_PORT = int(os.environ.get("E2E_REDIS_PORT", "16379"))
_KAFKA_HOST = os.environ.get("E2E_KAFKA_HOST", "localhost")
_CH_HOST = os.environ.get("E2E_CH_HOST", "localhost")
# Compose remaps CH HTTP from 8123 to 8124 to dodge a host-side
# `clickhouse server` already on the workstation (see issue #24). CI binds
# CH directly on 8123, so it overrides via E2E_CH_HTTP_PORT=8123.
_CH_HTTP_PORT = int(os.environ.get("E2E_CH_HTTP_PORT", "8124"))


def _can_connect(host: str, port: int, timeout: float = 0.5) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(timeout)
        try:
            s.connect((host, port))
            return True
        except OSError:
            return False


_unreachable_services: list[str] = []
for _name, _host, _port in (
    ("postgres", _PG_HOST, _PG_PORT),
    ("redis", _REDIS_HOST, _REDIS_PORT),
    ("kafka", _KAFKA_HOST, 9092),
    ("clickhouse-http", _CH_HOST, _CH_HTTP_PORT),
):
    if not _can_connect(_host, _port):
        _unreachable_services.append(f"{_name} ({_host}:{_port})")

if _unreachable_services and os.environ.get("E2E_REQUIRE") == "1":
    # CI sets E2E_REQUIRE=1 — treat unreachable services as a hard failure
    # rather than a silent skip. A green-but-skipped CI pipeline is worse
    # than a red one because it pretends to test something it isn't.
    raise RuntimeError(
        "E2E_REQUIRE=1 but services not reachable: " + ", ".join(_unreachable_services)
    )

pytestmark = pytest.mark.skipif(
    bool(_unreachable_services),
    reason=f"local stack not reachable ({', '.join(_unreachable_services)}) — "
    "run `make compose-up` first",
)


# Fixed test ports distinct from any conflicts (ClickHouse / IDE / live
# `make compose-up` ocpp container which uses 9000 and 50051).
_TEST_WS_PORT = 19432
_TEST_GRPC_PORT = 19433
_TEST_DB_URL = f"postgresql+asyncpg://eveys:eveys@{_PG_HOST}:{_PG_PORT}/eveys_ocpp"


@pytest.fixture
def compose_endpoints() -> Iterator[dict[str, str]]:
    yield {
        "postgres": f"{_PG_HOST}:{_PG_PORT}",
        "redis": f"{_REDIS_HOST}:{_REDIS_PORT}",
        "kafka": f"{_KAFKA_HOST}:9092",
        "clickhouse": f"{_CH_HOST}:{_CH_HTTP_PORT}",
    }


def test_each_endpoint_is_reachable(compose_endpoints: dict[str, str]) -> None:
    for name, addr in compose_endpoints.items():
        host, port = addr.split(":")
        assert _can_connect(host, int(port)), f"{name} not reachable at {addr}"


def test_clickhouse_responds_ok() -> None:
    """ClickHouse `/ping` must return `Ok.\\n`."""
    import urllib.request

    with urllib.request.urlopen(f"http://{_CH_HOST}:{_CH_HTTP_PORT}/ping", timeout=2) as resp:
        body = resp.read().decode()
    assert body.strip() == "Ok."


# --------------------------------------------------------------------------
# E1-13 — full charger-simulator round-trip
# --------------------------------------------------------------------------


@pytest.fixture
async def running_service() -> AsyncIterator[None]:
    """Spawn `serve_forever` as a background task; tear it down after the test.

    Skips the entire test if the schema isn't applied — the test relies on
    `charge_points` and `transactions` tables existing.
    """
    # If schema isn't applied: skip on dev workstation, hard-fail in CI. CI runs
    # `alembic upgrade head` before pytest (see tests:e2e in .gitlab-ci.yml);
    # if that step silently no-op'd, this test would otherwise green-skip.
    engine = create_async_engine(_TEST_DB_URL)
    async with engine.connect() as conn:
        try:
            await conn.execute(sa.text("SELECT 1 FROM charge_points LIMIT 1"))
            await conn.execute(sa.text("SELECT 1 FROM transactions LIMIT 1"))
        except Exception:
            msg = "schema not applied — run `alembic upgrade head` first"
            if os.environ.get("E2E_REQUIRE") == "1":
                pytest.fail(
                    f"{msg}. E2E_REQUIRE=1 — `alembic upgrade head` should "
                    "have run in the tests:e2e job before pytest.",
                    pytrace=False,
                )
            pytest.skip(msg)
    await engine.dispose()

    # Override settings via env BEFORE importing the entry-point dependencies.
    # `os` is already imported at module scope; we don't reimport here (a
    # second `import os` inside the function shadows the module-level name
    # for the whole function and breaks the earlier reference above).
    # Saved values are restored on teardown so other tests in the same
    # pytest run see the process-default settings.
    saved_env = {
        k: os.environ.get(k)
        for k in (
            "EVEYS_OCPP_WS_PORT",
            "EVEYS_OCPP_GRPC_PORT",
            "EVEYS_OCPP_DB_URL",
            "EVEYS_OCPP_REDIS_URL",
            "EVEYS_OCPP_KAFKA_BROKERS",
            "EVEYS_OCPP_LOG_JSON",
        )
    }
    os.environ["EVEYS_OCPP_WS_PORT"] = str(_TEST_WS_PORT)
    os.environ["EVEYS_OCPP_GRPC_PORT"] = str(_TEST_GRPC_PORT)
    os.environ["EVEYS_OCPP_DB_URL"] = _TEST_DB_URL
    os.environ["EVEYS_OCPP_REDIS_URL"] = f"redis://{_REDIS_HOST}:6379/0"
    os.environ["EVEYS_OCPP_KAFKA_BROKERS"] = f"{_KAFKA_HOST}:9092"
    os.environ["EVEYS_OCPP_LOG_JSON"] = "false"

    from eveys_ocpp.connections import ConnectionMap
    from eveys_ocpp.events import KafkaEventProducer
    from eveys_ocpp.pending_authorizations import PendingAuthorizations
    from eveys_ocpp.persistence.db import make_engine, make_session_factory
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import get_settings
    from eveys_ocpp.transport.grpc_server import OcppGatewayService
    from eveys_ocpp.transport.grpc_server import serve_forever as serve_grpc_forever
    from eveys_ocpp.transport.ws_server import serve_forever as serve_ws_forever

    settings = get_settings()
    db_engine = make_engine(settings.db_url.get_secret_value())
    session_factory = make_session_factory(db_engine)
    registry = Registry.from_settings(settings)
    connections = ConnectionMap()
    event_producer = KafkaEventProducer.from_settings(settings)
    await event_producer.start()

    # The pending-auth store + IP rate limiter are required kwargs on
    # `serve_forever` now, but this smoke stack only exercises already-
    # authorized chargers, so a store rooted in the same Redis and a
    # `None` IP limiter is enough.
    from redis.asyncio import Redis as _Redis

    _redis_for_pending = _Redis.from_url(settings.redis_url, decode_responses=True)
    pending_store = PendingAuthorizations(_redis_for_pending, settings=settings)

    ws_task = asyncio.create_task(
        serve_ws_forever(
            session_factory=session_factory,
            settings=settings,
            registry=registry,
            connections=connections,
            event_producer=event_producer,
            pending_store=pending_store,
            ip_rate_limiter=None,
        )
    )
    command_service = OcppGatewayService(
        session_factory=session_factory,
        settings=settings,
        connections=connections,
        registry=registry,
        bus=None,
    )
    grpc_task = asyncio.create_task(serve_grpc_forever(settings=settings, service=command_service))
    # Give both servers a beat to bind.
    await asyncio.sleep(0.2)

    try:
        yield
    finally:
        ws_task.cancel()
        grpc_task.cancel()
        with suppress(asyncio.CancelledError, Exception):
            await ws_task
        with suppress(asyncio.CancelledError, Exception):
            await grpc_task
        await event_producer.stop()
        await registry.close()
        await db_engine.dispose()
        # Restore env so subsequent tests see process-default settings.
        for key, original in saved_env.items():
            if original is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = original


class _SimChargePoint(Cp):
    """Minimal client side of the OCPP library — sends, receives, that's it."""


@pytest.fixture
async def db_engine() -> AsyncIterator[sa.ext.asyncio.AsyncEngine]:
    engine = create_async_engine(_TEST_DB_URL)
    yield engine
    await engine.dispose()


async def _drive_full_transaction(cp_id: str) -> int:
    """Connect a sim, drive Boot→Auth→Start→Stop, return the transaction_id."""
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _SimChargePoint(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())

        boot = await sim.call(
            call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1")
        )
        assert boot.status == "Accepted"

        auth = await sim.call(call.Authorize(id_tag="VALID_RFID_001"))
        assert auth.id_tag_info["status"] == "Accepted"

        start = await sim.call(
            call.StartTransaction(
                connector_id=1,
                id_tag="VALID_RFID_001",
                meter_start=0,
                timestamp=datetime.now(UTC).isoformat(),
            )
        )
        assert start.id_tag_info["status"] == "Accepted"
        assert start.transaction_id > 0

        stop = await sim.call(
            call.StopTransaction(
                meter_stop=12345,
                timestamp=datetime.now(UTC).isoformat(),
                transaction_id=start.transaction_id,
                reason="Local",
                id_tag="VALID_RFID_001",
            )
        )
        assert stop.id_tag_info["status"] == "Accepted"

        loop_task.cancel()
        return int(start.transaction_id)


@pytest.mark.asyncio
async def test_full_charger_round_trip(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E1-13 — drive a full transaction; assert DB rows match the wire activity."""
    cp_id = "SMOKE_E1_13_001"

    transaction_id = await _drive_full_transaction(cp_id)

    # Verify charger row was upserted by BootNotification.
    async with db_engine.connect() as conn:
        cp_row = (
            await conn.execute(
                sa.text(
                    "SELECT cp_id, vendor, model, last_boot_at FROM charge_points "
                    "WHERE cp_id = :cp_id"
                ),
                {"cp_id": cp_id},
            )
        ).one()
    assert cp_row.cp_id == cp_id
    assert cp_row.vendor == "ACME"
    assert cp_row.model == "X1"
    assert cp_row.last_boot_at is not None

    # Verify transaction row reflects Start + Stop.
    async with db_engine.connect() as conn:
        tx_row = (
            await conn.execute(
                sa.text(
                    "SELECT transaction_id, connector_id, id_tag, meter_start_wh, "
                    "meter_stop_wh, stop_reason "
                    "FROM transactions WHERE transaction_id = :tx_id"
                ),
                {"tx_id": transaction_id},
            )
        ).one()
    assert tx_row.transaction_id == transaction_id
    assert tx_row.connector_id == 1
    assert tx_row.id_tag == "VALID_RFID_001"
    assert tx_row.meter_start_wh == 0
    assert tx_row.meter_stop_wh == 12345
    assert tx_row.stop_reason == "Local"


@pytest.mark.asyncio
async def test_stop_transaction_replay_is_idempotent(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """A duplicate StopTransaction (same cp_id/tx_id/meter_stop) must not double-write."""
    cp_id = "SMOKE_E1_13_002"

    transaction_id = await _drive_full_transaction(cp_id)

    # Second StopTransaction with the same triple → idempotency key collision.
    # The handler still replies Accepted (charger doesn't need to know it's a
    # replay); the DB row should be unchanged.
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _SimChargePoint(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())

        replay_stop = await sim.call(
            call.StopTransaction(
                meter_stop=12345,
                timestamp=datetime.now(UTC).isoformat(),
                transaction_id=transaction_id,
                reason="Local",
                id_tag="VALID_RFID_001",
            )
        )
        assert replay_stop.id_tag_info["status"] == "Accepted"
        loop_task.cancel()

    # DB state should be unchanged: still one row, still meter_stop_wh=12345.
    async with db_engine.connect() as conn:
        row_count = (
            await conn.execute(
                sa.text("SELECT COUNT(*) FROM transactions WHERE transaction_id = :tx_id"),
                {"tx_id": transaction_id},
            )
        ).scalar_one()
    assert row_count == 1


@pytest.mark.asyncio
async def test_registry_marks_charger_online_then_offline(
    running_service: None,
) -> None:
    """E2-9 — connect a sim, assert `cp:online:{cp_id}` exists in Redis;
    disconnect, assert the key is gone (compare-and-delete on our pod_id).
    """
    import socket as _socket

    from redis.asyncio import Redis

    cp_id = "SMOKE_E2_9_001"
    redis = Redis.from_url(f"redis://{_REDIS_HOST}:6379/0", decode_responses=True)
    try:
        # No leftover from previous runs (idempotent).
        await redis.delete(f"cp:online:{cp_id}")

        async with connect(
            f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]
        ) as ws:
            sim = _SimChargePoint(cp_id, ws)
            loop_task = asyncio.create_task(sim.start())
            await sim.call(
                call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1")
            )

            # Boot succeeded → registry key must exist with our pod_id.
            value = await redis.get(f"cp:online:{cp_id}")
            assert value == _socket.gethostname(), (
                f"registry key missing or wrong owner: got {value!r}"
            )

            # TTL was set (must be ≤ configured TTL; default 120).
            ttl = await redis.ttl(f"cp:online:{cp_id}")
            assert 0 < ttl <= 120

            loop_task.cancel()

        # WS closed → mark_offline ran → key should be gone.
        await asyncio.sleep(0.1)  # mark_offline runs in finally
        value_after = await redis.get(f"cp:online:{cp_id}")
        assert value_after is None, f"registry key not cleaned up: {value_after!r}"
    finally:
        await redis.aclose()


@pytest.mark.asyncio
async def test_meter_values_publishes_to_kafka(
    running_service: None,
) -> None:
    """E2-1 — sim sends MeterValues; envelope should land in `cp.meter`.

    Uses a fresh aiokafka consumer subscribed to `cp.meter` from
    `latest`; we need to subscribe BEFORE the sim publishes so we don't
    miss the message.
    """
    from aiokafka import AIOKafkaConsumer

    from eveys_ocpp._generated.events.v1 import events_pb2

    cp_id = "SMOKE_E2_1_001"
    consumer = AIOKafkaConsumer(
        "cp.meter",
        bootstrap_servers=f"{_KAFKA_HOST}:9092",
        # Read only messages produced from now onward.
        auto_offset_reset="latest",
        group_id=None,  # standalone consumer; no group coordination
        enable_auto_commit=False,
    )
    await consumer.start()
    try:
        # Give the broker a beat to register the assignment.
        await asyncio.sleep(0.5)

        async with connect(
            f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]
        ) as ws:
            sim = _SimChargePoint(cp_id, ws)
            loop_task = asyncio.create_task(sim.start())

            # Boot first so the cp row exists (BootNotification handler
            # writes to Postgres; not strictly required for MeterValues
            # but mirrors a real charger's lifecycle).
            await sim.call(
                call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1")
            )

            from ocpp.v16.call import MeterValues as _MeterValuesCall

            await sim.call(
                _MeterValuesCall(
                    connector_id=2,
                    transaction_id=999,
                    meter_value=[
                        {
                            "timestamp": "2026-04-30T01:23:45+00:00",
                            "sampled_value": [
                                {
                                    "value": "5421",
                                    "unit": "Wh",
                                    "measurand": "Energy.Active.Import.Register",
                                },
                                {
                                    "value": "11.2",
                                    "unit": "A",
                                    "measurand": "Current.Import",
                                },
                            ],
                        }
                    ],
                )
            )

            loop_task.cancel()

        # Now consume — there should be exactly one record on `cp.meter`
        # for our cp_id. Wait up to 5 seconds.
        deadline = asyncio.get_event_loop().time() + 5.0
        record = None
        while asyncio.get_event_loop().time() < deadline:
            msg_batch = await consumer.getmany(timeout_ms=500, max_records=10)
            for _tp, records in msg_batch.items():
                for r in records:
                    if r.key == cp_id.encode("utf-8"):
                        record = r
                        break
                if record is not None:
                    break
            if record is not None:
                break

        assert record is not None, "no record on cp.meter for our cp_id"
        envelope = events_pb2.EventEnvelope()
        envelope.ParseFromString(record.value)
        assert envelope.cp_id == cp_id
        assert envelope.HasField("cp_meter")
        assert envelope.cp_meter.connector_id == 2
        assert envelope.cp_meter.transaction_id == 999
        assert len(envelope.cp_meter.sampled_values) == 2
    finally:
        await consumer.stop()


@pytest.mark.asyncio
async def test_grpc_remote_start_dispatches_to_charger(
    running_service: None,
) -> None:
    """E2-5 — call gRPC RemoteStart against a connected sim, verify the
    sim received the OCPP RemoteStartTransaction.req and the gRPC reply
    carries the charger's status.
    """
    import asyncio as _asyncio

    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    cp_id = "SMOKE_E2_5_001"
    received: list[tuple[str, int | None]] = []

    class _SimWithRemoteStart(_SimChargePoint):
        @_on(_Action.remote_start_transaction)
        async def on_remote_start(  # type: ignore[no-untyped-def]
            self, id_tag: str, connector_id: int | None = None, **_: object
        ):
            received.append((id_tag, connector_id))
            return _call_result.RemoteStartTransaction(status="Accepted")

    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _SimWithRemoteStart(cp_id, ws)
        loop_task = _asyncio.create_task(sim.start())
        # Boot first so the charger row exists and the connection is
        # tracked in the in-process ConnectionMap.
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        # Now hit gRPC RemoteStart against the running service. The
        # service is on this same pod; ConnectionMap.get(cp_id) will
        # return our sim's EveysChargePoint, which forwards to our WS.
        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStart(
                _gateway_pb2.RemoteStartRequest(
                    cp_id=cp_id, id_tag="VALID_RFID_001", connector_id=2
                )
            )

        assert response.status == _gateway_pb2.REMOTE_START_STATUS_ACCEPTED
        # Sim received the OCPP request with the right fields.
        assert received == [("VALID_RFID_001", 2)]

        loop_task.cancel()


@pytest.mark.asyncio
async def test_grpc_remote_start_offline_charger_returns_not_found(
    running_service: None,
) -> None:
    """No sim connected → registry has no key → NOT_FOUND."""
    from grpclib.client import Channel as _Channel
    from grpclib.const import Status as _Status
    from grpclib.exceptions import GRPCError as _GRPCError

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
        stub = _gateway_grpc.OcppGatewayStub(ch)
        with pytest.raises(_GRPCError) as exc:
            await stub.RemoteStart(_gateway_pb2.RemoteStartRequest(cp_id="NEVER_SEEN", id_tag="X"))
    assert exc.value.status == _Status.NOT_FOUND


@pytest.mark.asyncio
async def test_grpc_remote_stop_dispatches_to_charger(
    running_service: None,
) -> None:
    """E2-6 — gRPC RemoteStop reaches the charger and the reply translates."""
    import asyncio as _asyncio

    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    cp_id = "SMOKE_E2_6_001"
    received_tx: list[int] = []

    class _SimWithRemoteStop(_SimChargePoint):
        @_on(_Action.remote_stop_transaction)
        async def on_remote_stop(  # type: ignore[no-untyped-def]
            self, transaction_id: int, **_: object
        ):
            received_tx.append(transaction_id)
            return _call_result.RemoteStopTransaction(status="Accepted")

    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _SimWithRemoteStop(cp_id, ws)
        loop_task = _asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.RemoteStop(
                _gateway_pb2.RemoteStopRequest(cp_id=cp_id, transaction_id=12345)
            )

        assert response.status == _gateway_pb2.REMOTE_STOP_STATUS_ACCEPTED
        assert received_tx == [12345]

        loop_task.cancel()


@pytest.mark.asyncio
async def test_grpc_get_charger_status_offline(
    running_service: None,
) -> None:
    """E2-6 — GetChargerStatus answers from registry+postgres, no OCPP round-trip.

    A charger that has never connected yields online=False, empty pod_id,
    empty last_status, empty last_heartbeat_at.
    """
    from grpclib.client import Channel as _Channel

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
        stub = _gateway_grpc.OcppGatewayStub(ch)
        response = await stub.GetChargerStatus(
            _gateway_pb2.GetChargerStatusRequest(cp_id="NEVER_SEEN_BY_E2_6")
        )

    assert response.cp_id == "NEVER_SEEN_BY_E2_6"
    assert response.online is False
    assert response.pod_id == ""
    assert response.last_status == ""
    assert response.last_heartbeat_at == ""


@pytest.mark.asyncio
async def test_grpc_get_charger_status_online_with_cached_state(
    running_service: None,
) -> None:
    """Connect a sim, send Boot + StatusNotification, then read GetChargerStatus.

    Verifies the registry online flag + the Postgres-cached `last_status`.
    """
    import asyncio as _asyncio

    from grpclib.client import Channel as _Channel

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    cp_id = "SMOKE_E2_6_STATUS_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _SimChargePoint(cp_id, ws)
        loop_task = _asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))
        await sim.call(
            call.StatusNotification(
                connector_id=1,
                error_code="NoError",
                status="Available",
            )
        )

        # Tiny sleep to let the WS handler's Postgres write commit before we read.
        await _asyncio.sleep(0.1)

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetChargerStatus(
                _gateway_pb2.GetChargerStatusRequest(cp_id=cp_id)
            )

        assert response.online is True
        assert response.pod_id != ""  # whatever this test process registered as
        assert response.last_status == "Available"

        loop_task.cancel()


# --------------------------------------------------------------------------
# E2-1B — LocalAuthList round-trip
# --------------------------------------------------------------------------


class _LocalAuthListSimHandler:
    """Charger-side OCPP handlers for LocalAuthList. Captures every
    `SendLocalList` and `GetLocalListVersion` request the gateway sends
    and replies with configurable status / version values so the test
    can assert end-to-end behaviour."""

    def __init__(self, *, send_status: str = "Accepted", reported_version: int = 0) -> None:
        self.send_received: list[dict[str, object]] = []
        self.get_received: int = 0
        self.send_status = send_status
        self.reported_version = reported_version

    @staticmethod
    def _on_send_local_list(self: object, **kwargs: object) -> object:
        # Static so the @on decorator can find it; bound at __init_subclass__.
        raise NotImplementedError


@pytest.mark.asyncio
async def test_local_auth_list_full_replace_persists_mirror(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1B — gRPC SendLocalList(Full, Accepted) → charger receives it →
    gateway-side `local_auth_lists` + `local_auth_list_entries` tables
    reflect the new state.
    """
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    captured: dict[str, object] = {}

    class _Sim(_SimChargePoint):
        @_on(_Action.send_local_list)
        async def _on_send_local_list(
            self,
            list_version: int,
            update_type: str,
            local_authorization_list: list[dict[str, object]] | None = None,
            **_kw: object,
        ) -> _call_result.SendLocalList:
            captured["list_version"] = list_version
            captured["update_type"] = update_type
            captured["entries"] = local_authorization_list or []
            return _call_result.SendLocalList(status="Accepted")

    cp_id = "SMOKE_E2_1B_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())

        # Charger row must exist before SendLocalList — it's the FK
        # target for `local_auth_lists`. BootNotification creates it.
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SendLocalList(
                _gateway_pb2.SendLocalListRequest(
                    cp_id=cp_id,
                    list_version=11,
                    update_type=_gateway_pb2.LOCAL_AUTH_LIST_UPDATE_TYPE_FULL,
                    local_authorization_list=[
                        _gateway_pb2.AuthorizationData(
                            id_tag="TAG_ALPHA",
                            id_tag_info=_gateway_pb2.IdTagInfo(
                                status=_gateway_pb2.AUTHORIZATION_STATUS_ACCEPTED,
                                parent_id_tag="FAMILY_1",
                            ),
                        ),
                        _gateway_pb2.AuthorizationData(
                            id_tag="TAG_BETA",
                            id_tag_info=_gateway_pb2.IdTagInfo(
                                status=_gateway_pb2.AUTHORIZATION_STATUS_BLOCKED
                            ),
                        ),
                    ],
                )
            )

        assert response.status == _gateway_pb2.SEND_LOCAL_LIST_STATUS_ACCEPTED

        # The charger sim received exactly what we sent.
        assert captured["list_version"] == 11
        assert captured["update_type"] == "Full"
        entries = captured["entries"]
        assert isinstance(entries, list)
        assert len(entries) == 2

        # Tiny sleep to let the post-charger persist commit.
        await asyncio.sleep(0.1)

        # Gateway-side mirror reflects what the charger accepted.
        async with db_engine.connect() as conn:
            mirror_row = (
                await conn.execute(
                    sa.text(
                        "SELECT lal.list_version, lal.last_full_replace_at "
                        "FROM local_auth_lists lal "
                        "JOIN charge_points cp ON cp.id = lal.charge_point_id "
                        "WHERE cp.cp_id = :cp_id"
                    ),
                    {"cp_id": cp_id},
                )
            ).one()
            entries_rows = (
                await conn.execute(
                    sa.text(
                        "SELECT e.id_tag, e.status, e.parent_id_tag "
                        "FROM local_auth_list_entries e "
                        "JOIN local_auth_lists lal ON lal.id = e.local_auth_list_id "
                        "JOIN charge_points cp ON cp.id = lal.charge_point_id "
                        "WHERE cp.cp_id = :cp_id ORDER BY e.id_tag"
                    ),
                    {"cp_id": cp_id},
                )
            ).all()

        assert mirror_row.list_version == 11
        assert mirror_row.last_full_replace_at is not None
        assert [(r.id_tag, r.status, r.parent_id_tag) for r in entries_rows] == [
            ("TAG_ALPHA", "Accepted", "FAMILY_1"),
            ("TAG_BETA", "Blocked", None),
        ]

        loop_task.cancel()


@pytest.mark.asyncio
async def test_local_auth_list_get_version_reads_from_charger(
    running_service: None,
) -> None:
    """GetLocalListVersion is a charger round-trip — the gateway forwards
    whatever the charger reports (here: -1 to mean "no list"). The
    gateway-side mirror is for operator queries / Differential planning,
    not this RPC."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    class _Sim(_SimChargePoint):
        @_on(_Action.get_local_list_version)
        async def _on_get(self, **_kw: object) -> _call_result.GetLocalListVersion:
            return _call_result.GetLocalListVersion(list_version=-1)

    cp_id = "SMOKE_E2_1B_002"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetLocalListVersion(
                _gateway_pb2.GetLocalListVersionRequest(cp_id=cp_id)
            )

        assert response.list_version == -1
        loop_task.cancel()


# --------------------------------------------------------------------------
# E2-1C — Reservations round-trip (ADR-0021)
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_reserve_now_full_lifecycle(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1C — gRPC ReserveNow(Accepted) → charger receives the
    gateway-assigned reservation_id → `reservations` row flips
    Pending → Active. Then CancelReservation(Accepted) → row is
    Cancelled."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    captured: dict[str, object] = {}

    class _Sim(_SimChargePoint):
        @_on(_Action.reserve_now)
        async def _on_reserve_now(
            self,
            connector_id: int,
            expiry_date: str,
            id_tag: str,
            reservation_id: int,
            parent_id_tag: str | None = None,
            **_kw: object,
        ) -> _call_result.ReserveNow:
            captured["connector_id"] = connector_id
            captured["expiry_date"] = expiry_date
            captured["id_tag"] = id_tag
            captured["reservation_id"] = reservation_id
            captured["parent_id_tag"] = parent_id_tag
            return _call_result.ReserveNow(status="Accepted")

        @_on(_Action.cancel_reservation)
        async def _on_cancel(
            self, reservation_id: int, **_kw: object
        ) -> _call_result.CancelReservation:
            captured["cancel_reservation_id"] = reservation_id
            return _call_result.CancelReservation(status="Accepted")

    cp_id = "SMOKE_E2_1C_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            reserve_resp = await stub.ReserveNow(
                _gateway_pb2.ReserveNowRequest(
                    cp_id=cp_id,
                    connector_id=1,
                    expiry_date="2026-12-31T23:59:59+00:00",
                    id_tag="TAG_VIP",
                    parent_id_tag="FAMILY_1",
                )
            )

        assert reserve_resp.status == _gateway_pb2.RESERVE_NOW_STATUS_ACCEPTED
        assert reserve_resp.reservation_id > 0
        rid = reserve_resp.reservation_id

        # Charger received the gateway-assigned reservation_id.
        assert captured["reservation_id"] == rid
        assert captured["id_tag"] == "TAG_VIP"
        assert captured["parent_id_tag"] == "FAMILY_1"

        # Tiny sleep to let the post-charger Active flip commit.
        await asyncio.sleep(0.1)

        async with db_engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.text(
                        "SELECT r.status, r.id_tag, r.parent_id_tag, r.connector_id "
                        "FROM reservations r "
                        "JOIN charge_points cp ON cp.id = r.charge_point_id "
                        "WHERE cp.cp_id = :cp_id AND r.id = :rid"
                    ),
                    {"cp_id": cp_id, "rid": rid},
                )
            ).one()

        assert row.status == "Active"
        assert row.id_tag == "TAG_VIP"
        assert row.parent_id_tag == "FAMILY_1"
        assert row.connector_id == 1

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            cancel_resp = await stub.CancelReservation(
                _gateway_pb2.CancelReservationRequest(cp_id=cp_id, reservation_id=rid)
            )

        assert cancel_resp.status == _gateway_pb2.CANCEL_RESERVATION_STATUS_ACCEPTED
        assert captured["cancel_reservation_id"] == rid

        await asyncio.sleep(0.1)
        async with db_engine.connect() as conn:
            cancel_row = (
                await conn.execute(
                    sa.text("SELECT status FROM reservations WHERE id = :rid"),
                    {"rid": rid},
                )
            ).one()
        assert cancel_row.status == "Cancelled"

        loop_task.cancel()


@pytest.mark.asyncio
async def test_reserve_now_charger_occupied_drops_pending_row(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1C — charger reports Occupied → no `reservations` row
    survives (Pending was inserted to allocate the ID, then deleted
    when the charger refused). Mirrors ADR-0021 §"persist only on
    Accepted"."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    class _Sim(_SimChargePoint):
        @_on(_Action.reserve_now)
        async def _on_reserve_now(self, **_kw: object) -> _call_result.ReserveNow:
            return _call_result.ReserveNow(status="Occupied")

    cp_id = "SMOKE_E2_1C_002"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ReserveNow(
                _gateway_pb2.ReserveNowRequest(
                    cp_id=cp_id,
                    connector_id=1,
                    expiry_date="2026-12-31T23:59:59+00:00",
                    id_tag="TAG_LOSE",
                )
            )

        assert response.status == _gateway_pb2.RESERVE_NOW_STATUS_OCCUPIED
        rid = response.reservation_id
        assert rid > 0  # ID was still allocated by the gateway

        await asyncio.sleep(0.1)

        # No row survives — the Pending allocation was rolled back.
        async with db_engine.connect() as conn:
            count = (
                await conn.execute(
                    sa.text("SELECT count(*) AS n FROM reservations WHERE id = :rid"),
                    {"rid": rid},
                )
            ).scalar()
        assert count == 0

        loop_task.cancel()


# --------------------------------------------------------------------------
# E2-1F — Diagnostics + Firmware
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_diagnostics_get_then_status_notification(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1F — gRPC GetDiagnostics → charger replies with file_name →
    charger pushes DiagnosticsStatusNotification(Uploaded) → gateway
    persists into `charge_points.last_diagnostics_status`."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    captured: dict[str, object] = {}

    class _Sim(_SimChargePoint):
        @_on(_Action.get_diagnostics)
        async def _on_get_diag(self, location: str, **_kw: object) -> _call_result.GetDiagnostics:
            captured["location"] = location
            return _call_result.GetDiagnostics(file_name="diag-2026-05-05.tar.gz")

    cp_id = "SMOKE_E2_1F_DIAG_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        # Operator issues GetDiagnostics.
        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.GetDiagnostics(
                _gateway_pb2.GetDiagnosticsRequest(
                    cp_id=cp_id,
                    location="https://logs.eveys.example/incoming",
                )
            )

        assert response.file_name == "diag-2026-05-05.tar.gz"
        assert captured["location"] == "https://logs.eveys.example/incoming"

        # Charger now reports the upload-state machine progressing.
        await sim.call(call.DiagnosticsStatusNotification(status="Uploading"))
        await asyncio.sleep(0.1)
        async with db_engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.text(
                        "SELECT last_diagnostics_status FROM charge_points WHERE cp_id = :cp_id"
                    ),
                    {"cp_id": cp_id},
                )
            ).one()
        assert row.last_diagnostics_status == "Uploading"

        await sim.call(call.DiagnosticsStatusNotification(status="Uploaded"))
        await asyncio.sleep(0.1)
        async with db_engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.text(
                        "SELECT last_diagnostics_status FROM charge_points WHERE cp_id = :cp_id"
                    ),
                    {"cp_id": cp_id},
                )
            ).one()
        assert row.last_diagnostics_status == "Uploaded"

        loop_task.cancel()


@pytest.mark.asyncio
async def test_firmware_update_then_status_notifications(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1F — gRPC UpdateFirmware → charger Acks → charger walks the
    Downloading → Downloaded → Installing → Installed lifecycle, and
    `charge_points.last_firmware_status` mirrors the latest at each step."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    captured: dict[str, object] = {}

    class _Sim(_SimChargePoint):
        @_on(_Action.update_firmware)
        async def _on_update_fw(
            self, location: str, retrieve_date: str, **_kw: object
        ) -> _call_result.UpdateFirmware:
            captured["location"] = location
            captured["retrieve_date"] = retrieve_date
            return _call_result.UpdateFirmware()

    cp_id = "SMOKE_E2_1F_FW_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            await stub.UpdateFirmware(
                _gateway_pb2.UpdateFirmwareRequest(
                    cp_id=cp_id,
                    location="https://firmware.eveys.example/2026.05.bin",
                    retrieve_date="2026-05-05T03:00:00+00:00",
                )
            )

        assert captured["location"] == "https://firmware.eveys.example/2026.05.bin"
        assert captured["retrieve_date"] == "2026-05-05T03:00:00+00:00"

        # Charger walks the lifecycle. We verify the last column updates
        # at each step — the column is latest-wins.
        for status in ("Downloading", "Downloaded", "Installing", "Installed"):
            await sim.call(call.FirmwareStatusNotification(status=status))
            await asyncio.sleep(0.1)
            async with db_engine.connect() as conn:
                row = (
                    await conn.execute(
                        sa.text(
                            "SELECT last_firmware_status FROM charge_points WHERE cp_id = :cp_id"
                        ),
                        {"cp_id": cp_id},
                    )
                ).one()
            assert row.last_firmware_status == status

        loop_task.cancel()


# --------------------------------------------------------------------------
# E2-1E — Smart Charging round-trip (ADR-0022)
# --------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_set_charging_profile_persists_mirror(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1E — gRPC SetChargingProfile(Accepted) → charger receives the
    profile → `charging_profiles` row + `charging_schedule_periods`
    rows reflect the new state."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    captured: dict[str, object] = {}

    class _Sim(_SimChargePoint):
        @_on(_Action.set_charging_profile)
        async def _on_set(
            self,
            connector_id: int,
            cs_charging_profiles: dict[str, object],
            **_kw: object,
        ) -> _call_result.SetChargingProfile:
            captured["connector_id"] = connector_id
            captured["profile"] = cs_charging_profiles
            return _call_result.SetChargingProfile(status="Accepted")

    cp_id = "SMOKE_E2_1E_SET_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.SetChargingProfile(
                _gateway_pb2.SetChargingProfileRequest(
                    cp_id=cp_id,
                    connector_id=1,
                    cs_charging_profiles=_gateway_pb2.ChargingProfile(
                        charging_profile_id=42,
                        stack_level=1,
                        charging_profile_purpose=(
                            _gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_DEFAULT_PROFILE
                        ),
                        charging_profile_kind=_gateway_pb2.CHARGING_PROFILE_KIND_ABSOLUTE,
                        charging_schedule=_gateway_pb2.ChargingSchedule(
                            duration=3600,
                            charging_rate_unit=_gateway_pb2.CHARGING_RATE_UNIT_W,
                            charging_schedule_period=[
                                _gateway_pb2.ChargingSchedulePeriod(
                                    start_period=0, limit=11000.0, number_phases=3
                                ),
                                _gateway_pb2.ChargingSchedulePeriod(
                                    start_period=1800, limit=7400.0, number_phases=3
                                ),
                            ],
                        ),
                    ),
                )
            )

        assert response.status == _gateway_pb2.CHARGING_PROFILE_STATUS_ACCEPTED
        assert captured["connector_id"] == 1
        assert isinstance(captured["profile"], dict)

        await asyncio.sleep(0.1)

        async with db_engine.connect() as conn:
            profile_row = (
                await conn.execute(
                    sa.text(
                        "SELECT cp.id AS cp_pk, p.id AS profile_pk, "
                        "p.charging_profile_id, p.connector_id, "
                        "p.charging_profile_purpose, p.status "
                        "FROM charging_profiles p "
                        "JOIN charge_points cp ON cp.id = p.charge_point_id "
                        "WHERE cp.cp_id = :cp_id AND p.charging_profile_id = 42"
                    ),
                    {"cp_id": cp_id},
                )
            ).one()
            period_rows = (
                await conn.execute(
                    sa.text(
                        'SELECT start_period, "limit", number_phases '
                        "FROM charging_schedule_periods "
                        "WHERE charging_profile_id = :pid "
                        "ORDER BY start_period"
                    ),
                    {"pid": profile_row.profile_pk},
                )
            ).all()

        assert profile_row.connector_id == 1
        assert profile_row.charging_profile_purpose == "TxDefaultProfile"
        assert profile_row.status == "Active"
        assert len(period_rows) == 2
        assert period_rows[0].start_period == 0
        assert period_rows[1].start_period == 1800
        assert int(period_rows[0].number_phases) == 3

        # Replace the same profile id with a different schedule —
        # wholesale schedule replace.
        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            await stub.SetChargingProfile(
                _gateway_pb2.SetChargingProfileRequest(
                    cp_id=cp_id,
                    connector_id=1,
                    cs_charging_profiles=_gateway_pb2.ChargingProfile(
                        charging_profile_id=42,
                        stack_level=1,
                        charging_profile_purpose=(
                            _gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_DEFAULT_PROFILE
                        ),
                        charging_profile_kind=_gateway_pb2.CHARGING_PROFILE_KIND_ABSOLUTE,
                        charging_schedule=_gateway_pb2.ChargingSchedule(
                            duration=1800,
                            charging_rate_unit=_gateway_pb2.CHARGING_RATE_UNIT_W,
                            charging_schedule_period=[
                                _gateway_pb2.ChargingSchedulePeriod(start_period=0, limit=22000.0),
                            ],
                        ),
                    ),
                )
            )

        await asyncio.sleep(0.1)
        async with db_engine.connect() as conn:
            new_periods = (
                await conn.execute(
                    sa.text(
                        "SELECT count(*) AS n FROM charging_schedule_periods "
                        "WHERE charging_profile_id = :pid"
                    ),
                    {"pid": profile_row.profile_pk},
                )
            ).scalar()
        assert new_periods == 1  # old 2 periods wiped, new 1 inserted

        loop_task.cancel()


@pytest.mark.asyncio
async def test_clear_charging_profile_marks_cleared(
    running_service: None, db_engine: sa.ext.asyncio.AsyncEngine
) -> None:
    """E2-1E — gRPC ClearChargingProfile(Accepted) → matching mirror
    rows flip Active → Cleared (not deleted)."""
    from grpclib.client import Channel as _Channel
    from ocpp.routing import on as _on
    from ocpp.v16 import call_result as _call_result
    from ocpp.v16.enums import Action as _Action

    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_grpc as _gateway_grpc
    from eveys_ocpp._generated.ocpp_gw.v1 import gateway_pb2 as _gateway_pb2

    class _Sim(_SimChargePoint):
        @_on(_Action.set_charging_profile)
        async def _on_set(self, **_kw: object) -> _call_result.SetChargingProfile:
            return _call_result.SetChargingProfile(status="Accepted")

        @_on(_Action.clear_charging_profile)
        async def _on_clear(self, **_kw: object) -> _call_result.ClearChargingProfile:
            return _call_result.ClearChargingProfile(status="Accepted")

    cp_id = "SMOKE_E2_1E_CLEAR_001"
    async with connect(f"ws://localhost:{_TEST_WS_PORT}/{cp_id}", subprotocols=["ocpp1.6"]) as ws:
        sim = _Sim(cp_id, ws)
        loop_task = asyncio.create_task(sim.start())
        await sim.call(call.BootNotification(charge_point_vendor="ACME", charge_point_model="X1"))

        # First, set a profile so there's something to clear.
        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            await stub.SetChargingProfile(
                _gateway_pb2.SetChargingProfileRequest(
                    cp_id=cp_id,
                    connector_id=1,
                    cs_charging_profiles=_gateway_pb2.ChargingProfile(
                        charging_profile_id=99,
                        stack_level=0,
                        charging_profile_purpose=(_gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_PROFILE),
                        charging_profile_kind=_gateway_pb2.CHARGING_PROFILE_KIND_RELATIVE,
                        charging_schedule=_gateway_pb2.ChargingSchedule(
                            duration=600,
                            charging_rate_unit=_gateway_pb2.CHARGING_RATE_UNIT_A,
                            charging_schedule_period=[
                                _gateway_pb2.ChargingSchedulePeriod(start_period=0, limit=16.0),
                            ],
                        ),
                    ),
                )
            )

        await asyncio.sleep(0.1)

        # Now clear by purpose.
        async with _Channel("127.0.0.1", _TEST_GRPC_PORT) as ch:
            stub = _gateway_grpc.OcppGatewayStub(ch)
            response = await stub.ClearChargingProfile(
                _gateway_pb2.ClearChargingProfileRequest(
                    cp_id=cp_id,
                    charging_profile_purpose=(_gateway_pb2.CHARGING_PROFILE_PURPOSE_TX_PROFILE),
                )
            )

        assert response.status == _gateway_pb2.CLEAR_CHARGING_PROFILE_STATUS_ACCEPTED

        await asyncio.sleep(0.1)
        async with db_engine.connect() as conn:
            row = (
                await conn.execute(
                    sa.text(
                        "SELECT p.status FROM charging_profiles p "
                        "JOIN charge_points cp ON cp.id = p.charge_point_id "
                        "WHERE cp.cp_id = :cp_id AND p.charging_profile_id = 99"
                    ),
                    {"cp_id": cp_id},
                )
            ).one()
        assert row.status == "Cleared"

        loop_task.cancel()
