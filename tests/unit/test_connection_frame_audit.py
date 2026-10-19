"""Audit the actual OCPP serialization/send boundary with an inert socket."""

from __future__ import annotations

import json
from typing import Any

import pytest
from ocpp.messages import CallResult
from ocpp.v16 import call

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.clickhouse.ingestor import _extract_transaction_id
from eveys_ocpp.connection import EveysChargePoint
from eveys_ocpp.settings import Settings


class _Socket:
    def __init__(self, trace: list[str], *, fail: bool = False):
        self.trace, self.fail = trace, fail
        self.sent: list[str] = []
        self.cp: EveysChargePoint | None = None

    async def send(self, raw: str) -> None:
        self.trace.append("send")
        if self.fail:
            raise OSError("inert send failed")
        self.sent.append(raw)
        frame = json.loads(raw)
        if frame[0] == 2:
            assert self.cp is not None
            self.cp._response_queue.put_nowait(CallResult(frame[1], {"status": "Accepted"}))


class _Producer:
    def __init__(self, trace: list[str]):
        self.trace = trace
        self.frames: list[Any] = []

    async def publish(self, **request: Any) -> None:
        self.trace.append("publish")
        self.frames.append(events_pb2.EventEnvelope.FromString(request["value"]).cp_ocpp_frame)


def _cp(*, fail: bool = False) -> tuple[EveysChargePoint, _Socket, _Producer, list[str]]:
    trace: list[str] = []
    socket, producer = _Socket(trace, fail=fail), _Producer(trace)
    cp = EveysChargePoint(
        "CP_AUDIT", socket, session_factory=None, settings=Settings(), event_producer=producer
    )
    socket.cp = cp
    return cp, socket, producer, trace


@pytest.mark.asyncio
@pytest.mark.parametrize("unique_id", [None, "EXPLICIT_ID"])
async def test_remote_stop_audit_is_the_sent_wire_with_the_actual_correlation_id(
    unique_id: str | None,
) -> None:
    cp, socket, producer, trace = _cp()

    response = await cp.call(call.RemoteStopTransaction(transaction_id=4823), unique_id=unique_id)

    assert response.status == "Accepted"
    assert trace == ["send", "publish"]
    assert len(producer.frames) == 1
    frame = producer.frames[0]
    assert frame.raw_payload == socket.sent[0]
    raw = json.loads(socket.sent[0])
    assert raw[1] and frame.message_id == raw[1]
    if unique_id is not None:
        assert raw[1] == "EXPLICIT_ID"
    assert raw[3] == {"transactionId": 4823}
    assert frame.action == "RemoteStopTransaction"
    assert _extract_transaction_id(frame.raw_payload) == 4823


@pytest.mark.asyncio
async def test_remote_start_audit_uses_library_camel_case_and_omits_null_fields() -> None:
    cp, socket, producer, _ = _cp()

    await cp.call(call.RemoteStartTransaction(id_tag="TEST_TAG", connector_id=None))

    assert producer.frames[0].raw_payload == socket.sent[0]
    assert json.loads(socket.sent[0])[3] == {"idTag": "TEST_TAG"}


@pytest.mark.asyncio
async def test_failed_socket_send_is_not_recorded_as_a_sent_command() -> None:
    cp, socket, producer, _ = _cp(fail=True)

    with pytest.raises(OSError, match="inert send failed"):
        await cp.call(call.RemoteStopTransaction(transaction_id=4823))

    assert socket.sent == []
    assert producer.frames == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw,message_type",
    [
        ('[3,"INBOUND_START",{"transactionId":4823,"idTagInfo":{"status":"Accepted"}}]', 3),
        ('[4,"INBOUND_START","InternalError","test",{}]', 4),
    ],
)
async def test_response_audit_preserves_wire_id_without_inventing_an_action(
    raw: str, message_type: int
) -> None:
    cp, socket, producer, trace = _cp()

    await cp._send(raw)

    assert trace == ["send", "publish"]
    assert producer.frames[0].raw_payload == socket.sent[0] == raw
    assert producer.frames[0].message_id == "INBOUND_START"
    assert producer.frames[0].message_type == message_type
    assert producer.frames[0].action == ""


@pytest.mark.parametrize(
    "raw",
    [
        '[2,"HISTORICAL","RemoteStopTransaction",{"transaction_id":4823}]',
        '[2,"CURRENT","RemoteStopTransaction",{"transactionId":4823}]',
    ],
)
def test_transaction_index_accepts_historical_snake_case_and_current_wire(raw: str) -> None:
    assert _extract_transaction_id(raw) == 4823


@pytest.mark.asyncio
async def test_payload_format_error_is_sent_with_the_ocpp_1_6_error_code() -> None:
    """OCPP 1.6 names the code FormationViolation; the library uses the
    2.0.1 name FormatViolation."""
    cp, socket, _, _ = _cp()

    await cp.route_message('[2,"bad-1","Heartbeat",{"unexpected":1}]')

    [raw] = socket.sent
    frame = json.loads(raw)
    assert frame[:3] == [4, "bad-1", "FormationViolation"]
    assert "unexpected" in frame[4]["cause"]


@pytest.mark.asyncio
async def test_other_frames_are_sent_unchanged() -> None:
    cp, socket, _, _ = _cp()
    sent = [
        '[4,"x","NotImplemented","Request Action is recognized but not supported",{}]',
        '[3,"y",{"status":"FormatViolation"}]',
    ]

    for raw in sent:
        await cp._send(raw)

    assert socket.sent == sent
