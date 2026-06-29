"""End-to-end: Kafka → ClickHouse round-trip via the ingestor sidecar.

Validates the Phase-2 exit gate: events published to Kafka appear in
ClickHouse "within seconds" (per `06-implementation-plan.md` line 148).

Instead of running the ingestor as an OS subprocess, this test
constructs the same `ClickHouseIngestor` class in-process and drives
its `ingest_loop` for a brief window. That keeps the test
self-contained but exercises the real Kafka and ClickHouse paths the
production sidecar would hit.

Skipped when Kafka or ClickHouse aren't reachable; hard-fails on
missing-but-required service when ``E2E_REQUIRE=1`` is set (CI).
"""

from __future__ import annotations

import asyncio
import os
import socket
import urllib.parse
import urllib.request
import uuid
from contextlib import closing

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.clickhouse.ingestor import ClickHouseIngestor
from eveys_ocpp.clickhouse.migrate import apply_pending
from eveys_ocpp.events import KafkaEventProducer
from eveys_ocpp.settings import Settings

_CH_HOST = os.environ.get("E2E_CH_HOST", "localhost")
_CH_HTTP_PORT = int(os.environ.get("E2E_CH_HTTP_PORT", "8124"))
# Native ClickHouse port. The container always listens on 9000 (CI's
# tests:e2e job reaches it as clickhouse:9000, the in-process compose
# stack uses container-side 9000 too). Dev laptops that run ClickHouse
# via `make compose-up` need to override this to 9001 because the
# compose docker-compose.yml host-maps 9000→9001 to avoid colliding
# with IDE tools that squat on 9000 (see deploy/compose/docker-compose.yml
# line ~123). Set EVEYS_OCPP_CLICKHOUSE_PORT=9001 OR
# E2E_CH_NATIVE_PORT=9001 in that case.
_CH_NATIVE_PORT = int(os.environ.get("E2E_CH_NATIVE_PORT", "9001"))
_CH_DB = os.environ.get("EVEYS_OCPP_CLICKHOUSE_DB", "eveys_ocpp")
_KAFKA_HOST = os.environ.get("E2E_KAFKA_HOST", "localhost")
_KAFKA_PORT = int(os.environ.get("E2E_KAFKA_PORT", "9092"))
_E2E_REQUIRE = os.environ.get("E2E_REQUIRE") == "1"


def _reachable(host: str, port: int) -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((host, port))
        except OSError:
            return False
        return True


_unreachable: list[str] = []
for _name, _host, _port in (
    ("clickhouse-http", _CH_HOST, _CH_HTTP_PORT),
    ("clickhouse-native", _CH_HOST, _CH_NATIVE_PORT),
    ("kafka", _KAFKA_HOST, _KAFKA_PORT),
):
    if not _reachable(_host, _port):
        _unreachable.append(f"{_name} ({_host}:{_port})")

if _unreachable:
    _msg = "Kafka→CH e2e needs: " + ", ".join(_unreachable)
    if _E2E_REQUIRE:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the tests:e2e job must keep its "
            "`clickhouse` and `kafka` services. CI config bug, not env issue.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


def _ch_query(sql: str) -> str:
    url = f"http://{_CH_HOST}:{_CH_HTTP_PORT}/?{urllib.parse.urlencode({'database': _CH_DB})}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8")


@pytest.mark.asyncio
async def test_meter_values_kafka_to_clickhouse_round_trip() -> None:
    """Publish a CpMeter envelope, run the ingestor briefly, assert
    the row landed.

    Uses a unique cp_id per test run (UUID-suffixed) so concurrent
    test runs don't interfere with each other.
    """
    # Schema must exist. apply_pending is idempotent — running it
    # here keeps the test self-contained even on a fresh ClickHouse.
    apply_pending(host=_CH_HOST, port=_CH_HTTP_PORT, db=_CH_DB)

    cp_id = f"E2E_{uuid.uuid4().hex[:8]}"
    event_id = str(uuid.uuid4())

    settings = Settings(
        kafka_brokers=f"{_KAFKA_HOST}:{_KAFKA_PORT}",
        clickhouse_host=_CH_HOST,
        clickhouse_port=_CH_NATIVE_PORT,
        clickhouse_db=_CH_DB,
        # Tight batch knobs so the test doesn't wait 5 s.
        clickhouse_ingestor_batch_max_seconds=1.0,
        clickhouse_ingestor_batch_size=10,
        # Unique consumer group per test run — fresh from earliest.
        clickhouse_ingestor_group=f"test-{uuid.uuid4().hex[:8]}",
    )

    # 1) Publish one envelope on cp.meter.
    producer = KafkaEventProducer.from_settings(settings)
    await producer.start()
    try:
        envelope = events_pb2.EventEnvelope(
            event_id=event_id,
            occurred_at="2026-05-01T00:00:00.000+00:00",
            cp_id=cp_id,
            schema_version="v1",
            trace_id="trace-test",
            cp_meter=events_pb2.CpMeter(
                connector_id=1,
                transaction_id=42,
                charger_reported_at="2026-05-01T00:00:00+00:00",
                sampled_values=[
                    events_pb2.SampledValue(
                        value="230.5",
                        measurand=events_pb2.MEASURAND_VOLTAGE,
                        unit=events_pb2.UNIT_V,
                    ),
                    events_pb2.SampledValue(
                        value="16.2",
                        measurand=events_pb2.MEASURAND_CURRENT_IMPORT,
                        unit=events_pb2.UNIT_A,
                    ),
                ],
            ),
        )
        await producer.publish(
            topic=settings.kafka_topic_cp_meter,
            key=cp_id,
            value=envelope.SerializeToString(),
        )
    finally:
        await producer.stop()

    # 2) Run the ingestor briefly. One getmany() with a 1 s timeout
    # should return the message (it's already on the broker by the
    # time `producer.stop()` returned).
    ingestor = ClickHouseIngestor(settings)
    await ingestor.start()
    try:
        loop_task = asyncio.create_task(ingestor.ingest_loop())
        # Poll ClickHouse for the row. Up to 10 s — generous, real
        # ingestion should land in <2 s with the tight batch knobs.
        for _ in range(20):
            await asyncio.sleep(0.5)
            body = _ch_query(
                f"SELECT count() FROM cp_meter WHERE cp_id = '{cp_id}' FORMAT TabSeparated"
            )
            count = int(body.strip() or "0")
            if count >= 1:
                break
        else:
            pytest.fail(f"row never landed in cp_meter for cp_id={cp_id} within 10 s")

        # 3) Assert the row shape — envelope columns + Nested arrays.
        body = _ch_query(
            f"SELECT event_id, connector_id, transaction_id, "
            f"sampled_values.value, sampled_values.measurand, sampled_values.unit "
            f"FROM cp_meter WHERE cp_id = '{cp_id}' FORMAT TabSeparated"
        )
        line = body.strip().splitlines()[0].split("\t")
        assert line[0] == event_id
        assert line[1] == "1"
        assert line[2] == "42"
        # Arrays come back as ClickHouse-formatted lists: ['230.5','16.2']
        assert "230.5" in line[3] and "16.2" in line[3]
        assert "MEASURAND_VOLTAGE" in line[4]
        assert "UNIT_V" in line[5] and "UNIT_A" in line[5]
    finally:
        loop_task.cancel()
        await asyncio.gather(loop_task, return_exceptions=True)
        await ingestor.stop()
