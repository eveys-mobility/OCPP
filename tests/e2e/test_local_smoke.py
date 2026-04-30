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
# laptop). In GitLab CI, where the stack runs as `services:` sidecars, the
# pipeline overrides these via env to the service aliases (e.g. `postgres`).
_PG_HOST = os.environ.get("E2E_PG_HOST", "localhost")
_REDIS_HOST = os.environ.get("E2E_REDIS_HOST", "localhost")
_KAFKA_HOST = os.environ.get("E2E_KAFKA_HOST", "localhost")
_CH_HOST = os.environ.get("E2E_CH_HOST", "localhost")


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
    ("postgres", _PG_HOST, 5432),
    ("redis", _REDIS_HOST, 6379),
    ("kafka", _KAFKA_HOST, 9092),
    ("clickhouse-http", _CH_HOST, 8123),
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


# Fixed test port distinct from any ClickHouse / IDE squat on 9000.
_TEST_WS_PORT = 19432
_TEST_DB_URL = f"postgresql+asyncpg://eveys:eveys@{_PG_HOST}:5432/eveys_ocpp"


@pytest.fixture
def compose_endpoints() -> Iterator[dict[str, str]]:
    yield {
        "postgres": f"{_PG_HOST}:5432",
        "redis": f"{_REDIS_HOST}:6379",
        "kafka": f"{_KAFKA_HOST}:9092",
        "clickhouse": f"{_CH_HOST}:8123",
    }


def test_each_endpoint_is_reachable(compose_endpoints: dict[str, str]) -> None:
    for name, addr in compose_endpoints.items():
        host, port = addr.split(":")
        assert _can_connect(host, int(port)), f"{name} not reachable at {addr}"


def test_clickhouse_responds_ok() -> None:
    """ClickHouse `/ping` must return `Ok.\\n`."""
    import urllib.request

    with urllib.request.urlopen(f"http://{_CH_HOST}:8123/ping", timeout=2) as resp:
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
    # Skip if schema not applied (alembic hasn't run against the compose DB).
    engine = create_async_engine(_TEST_DB_URL)
    async with engine.connect() as conn:
        try:
            await conn.execute(sa.text("SELECT 1 FROM charge_points LIMIT 1"))
            await conn.execute(sa.text("SELECT 1 FROM transactions LIMIT 1"))
        except Exception:
            pytest.skip("schema not applied — run `alembic upgrade head` first")
    await engine.dispose()

    # Override settings via env BEFORE importing the entry-point dependencies.
    # We use module-level imports inside this fixture so `Settings()` reads our
    # overrides, not the defaults from the running test process. Saved values
    # are restored on teardown so other tests in the same pytest run see the
    # process-default settings.
    import os

    saved_env = {
        k: os.environ.get(k)
        for k in (
            "EVEYS_OCPP_WS_PORT",
            "EVEYS_OCPP_DB_URL",
            "EVEYS_OCPP_REDIS_URL",
            "EVEYS_OCPP_KAFKA_BROKERS",
            "EVEYS_OCPP_LOG_JSON",
        )
    }
    os.environ["EVEYS_OCPP_WS_PORT"] = str(_TEST_WS_PORT)
    os.environ["EVEYS_OCPP_DB_URL"] = _TEST_DB_URL
    os.environ["EVEYS_OCPP_REDIS_URL"] = f"redis://{_REDIS_HOST}:6379/0"
    os.environ["EVEYS_OCPP_KAFKA_BROKERS"] = f"{_KAFKA_HOST}:9092"
    os.environ["EVEYS_OCPP_LOG_JSON"] = "false"

    from eveys_ocpp.events import KafkaEventProducer
    from eveys_ocpp.persistence.db import make_engine, make_session_factory
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import get_settings
    from eveys_ocpp.transport.ws_server import serve_forever

    settings = get_settings()
    db_engine = make_engine(settings.db_url)
    session_factory = make_session_factory(db_engine)
    registry = Registry.from_settings(settings)
    event_producer = KafkaEventProducer.from_settings(settings)
    await event_producer.start()

    server_task = asyncio.create_task(
        serve_forever(
            session_factory=session_factory,
            settings=settings,
            registry=registry,
            event_producer=event_producer,
        )
    )
    # Give the server a beat to bind.
    await asyncio.sleep(0.2)

    try:
        yield
    finally:
        server_task.cancel()
        # Cancellation surfaces as CancelledError; tolerate any teardown error.
        with suppress(asyncio.CancelledError, Exception):
            await server_task
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
