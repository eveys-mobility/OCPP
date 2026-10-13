"""Unit tests for the cp.ocpp_frames publish helpers (#212).

The chokepoints live in `connection.py`; their integration with the
mobilityhouse/ocpp `route_message` / `call` flow is exercised by the
compose-smoke and e2e suites. What's worth unit-testing here is the
helper's contract: envelope shape on the happy path, no-op when the
producer or master switch is off, best-effort guard under broker
drop, and the outbound payload-to-dict path.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.ocpp_frames import publish_inbound, publish_outbound
from eveys_ocpp.settings import Settings


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {
        "kafka_bootstrap_servers": "localhost:9092",
        "backend_base_url": "",
    }
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_inbound_publishes_envelope_on_happy_path() -> None:
    producer = AsyncMock()
    raw = '[2,"abc","BootNotification",{"chargePointVendor":"ACME","chargePointModel":"X1"}]'

    await publish_inbound(
        producer=producer,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg=raw,
        message_type_id=2,
        action="BootNotification",
        message_id="abc",
    )

    producer.publish.assert_awaited_once()
    kwargs = producer.publish.await_args.kwargs
    assert kwargs["topic"] == "cp.ocpp_frames"
    assert kwargs["key"] == "CP_A"
    envelope = events_pb2.EventEnvelope.FromString(kwargs["value"])
    frame = envelope.cp_ocpp_frame
    assert frame.direction == "inbound"
    assert frame.raw_payload == raw
    assert frame.message_id == "abc"
    assert frame.action == "BootNotification"
    assert frame.message_type == 2
    assert frame.ocpp_version == "ocpp1.6"


@pytest.mark.asyncio
async def test_inbound_response_has_empty_action() -> None:
    # CALLRESULT / CALLERROR don't carry the action name on the wire.
    producer = AsyncMock()
    await publish_inbound(
        producer=producer,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg='[3,"abc",{}]',
        message_type_id=3,
        action="",
        message_id="abc",
    )
    envelope = events_pb2.EventEnvelope.FromString(producer.publish.await_args.kwargs["value"])
    assert envelope.cp_ocpp_frame.action == ""
    assert envelope.cp_ocpp_frame.message_type == 3


@pytest.mark.asyncio
async def test_inbound_noop_when_producer_is_none() -> None:
    # No producer (unit-test stack) → nothing happens, no exception.
    await publish_inbound(
        producer=None,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg="[2]",
        message_type_id=2,
        action="X",
        message_id=None,
    )


@pytest.mark.asyncio
async def test_inbound_noop_when_master_switch_off() -> None:
    producer = AsyncMock()
    await publish_inbound(
        producer=producer,
        settings=_settings(kafka_publish_ocpp_frames=False),
        cp_id="CP_A",
        raw_msg="[2]",
        message_type_id=2,
        action="X",
        message_id=None,
    )
    producer.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_inbound_swallows_broker_error() -> None:
    # Best-effort: a broker outage must not propagate. The WS path
    # would otherwise stop processing frames whenever Kafka blinked.
    producer = AsyncMock()
    producer.publish.side_effect = RuntimeError("broker down")
    await publish_inbound(
        producer=producer,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg='[2,"x","X",{}]',
        message_type_id=2,
        action="X",
        message_id="x",
    )
    # No assertion: the test passes if the call returned without raising.


@pytest.mark.asyncio
async def test_outbound_publishes_the_exact_sent_payload() -> None:
    producer = AsyncMock()
    raw = '[2,"msg-42","RemoteStartTransaction",{"idTag":"TAG-1","connectorId":1}]'

    await publish_outbound(
        producer=producer,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg=raw,
    )

    producer.publish.assert_awaited_once()
    envelope = events_pb2.EventEnvelope.FromString(producer.publish.await_args.kwargs["value"])
    frame = envelope.cp_ocpp_frame
    assert frame.direction == "outbound"
    assert frame.action == "RemoteStartTransaction"
    assert frame.message_id == "msg-42"
    assert frame.message_type == 2

    assert frame.raw_payload == raw
    parsed = json.loads(frame.raw_payload)
    assert parsed[0] == 2
    assert parsed[1] == "msg-42"
    assert parsed[2] == "RemoteStartTransaction"
    assert parsed[3] == {"idTag": "TAG-1", "connectorId": 1}


@pytest.mark.asyncio
async def test_outbound_swallows_broker_error() -> None:
    producer = AsyncMock()
    producer.publish.side_effect = RuntimeError("broker down")
    await publish_outbound(
        producer=producer,
        settings=_settings(),
        cp_id="CP_A",
        raw_msg='[2,"x","RemoteStartTransaction",{"idTag":"t","connectorId":1}]',
    )
