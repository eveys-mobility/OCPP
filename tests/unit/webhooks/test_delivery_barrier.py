"""Kafka ACK boundaries exercised without broker, HTTP listener or application boot."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest
from aiokafka.errors import IllegalStateError
from aiokafka.structs import TopicPartition

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp.settings import Settings
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher


@pytest.mark.asyncio
@pytest.mark.parametrize("disabled,missing", [(True, False), (False, True)])
async def test_failed_http_without_committed_backlog_cannot_ack(disabled, missing):
    dispatcher = WebhookDispatcher(
        Settings(_env_file=None, webhook_max_attempts=1, webhook_backlog_enabled=not disabled),
        session_factory=None if missing else lambda: None,
    )
    dispatcher._http = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(503))
    )
    try:
        with pytest.raises(RuntimeError):
            await dispatcher._post_with_retry(
                url="https://test.invalid/stop",
                body_bytes=b"{}",
                signature="test",
                event_id=str(uuid4()),
                event_type="tx.stopped",
            )
    finally:
        await dispatcher._http.aclose()


@pytest.mark.asyncio
async def test_failed_record_retains_batch_and_commits_only_explicit_delivered_offsets():
    dispatcher = WebhookDispatcher(Settings(_env_file=None))
    tp0, tp1 = TopicPartition("tx.stopped", 0), TopicPartition("tx.stopped", 1)
    batch = {
        tp0: [
            SimpleNamespace(topic=tp0.topic, offset=10),
            SimpleNamespace(topic=tp0.topic, offset=11),
        ],
        tp1: [SimpleNamespace(topic=tp1.topic, offset=20)],
    }
    delivered, commits = [], []
    failed = False

    async def deliver(record):
        nonlocal failed
        delivered.append(record.offset)
        if record.offset == 11 and not failed:
            failed = True
            raise RuntimeError("backlog commit failed")

    async def commit(offsets=None):
        commits.append(offsets)
        if tp1 in offsets:
            dispatcher._shutdown.set()

    dispatcher._consumer = SimpleNamespace(
        getmany=AsyncMock(return_value=batch), commit=commit, assignment=lambda: set(batch)
    )
    dispatcher._http = object()
    dispatcher._deliver_one = deliver
    dispatcher._sample_consumer_lag = AsyncMock()
    await asyncio.wait_for(dispatcher.serve_forever(), timeout=3)
    assert delivered == [10, 11, 11, 20]
    dispatcher._consumer.getmany.assert_awaited_once()
    assert [{tp: offset.offset for tp, offset in ack.items()} for ack in commits] == [
        {tp0: 11},
        {tp0: 12},
        {tp1: 21},
    ]


@pytest.mark.asyncio
async def test_malformed_stable_stop_is_retained_instead_of_silently_dropped():
    dispatcher = WebhookDispatcher(Settings(_env_file=None))
    with pytest.raises(RuntimeError):
        await dispatcher._deliver_one(
            SimpleNamespace(topic="tx.stopped", offset=1, value=b"bad protobuf")
        )
    envelope = events_pb2.EventEnvelope(
        event_id="invalid-id", cp_id="CP_TEST", tx_stopped=events_pb2.TxStopped(transaction_id=1)
    )
    with pytest.raises(RuntimeError):
        await dispatcher._deliver_one(
            SimpleNamespace(topic="tx.stopped", offset=2, value=envelope.SerializeToString())
        )


@pytest.mark.asyncio
async def test_failed_kafka_commit_retries_same_offsets_without_repeating_http():
    dispatcher = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    record = SimpleNamespace(topic=tp.topic, offset=5)
    commits = []

    async def commit(offsets):
        commits.append(offsets)
        if len(commits) == 1:
            raise RuntimeError("broker ACK lost")
        dispatcher._shutdown.set()

    dispatcher._consumer = SimpleNamespace(
        getmany=AsyncMock(return_value={tp: [record]}), commit=commit, assignment=lambda: {tp}
    )
    dispatcher._http = object()
    dispatcher._deliver_one = AsyncMock()
    dispatcher._sample_consumer_lag = AsyncMock()
    await asyncio.wait_for(dispatcher.serve_forever(), timeout=3)
    dispatcher._consumer.getmany.assert_awaited_once()
    dispatcher._deliver_one.assert_awaited_once_with(record)
    assert commits[0] == commits[1]
    assert commits[0][tp].offset == 6


@pytest.mark.asyncio
async def test_external_delivery_cancellation_does_not_commit_offsets():
    dispatcher = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    entered = asyncio.Event()

    async def deliver(_):
        entered.set()
        await asyncio.Event().wait()

    dispatcher._consumer = SimpleNamespace(
        getmany=AsyncMock(return_value={tp: [SimpleNamespace(topic=tp.topic, offset=5)]}),
        commit=AsyncMock(),
        assignment=lambda: {tp},
    )
    dispatcher._http = object()
    dispatcher._deliver_one = deliver
    task = asyncio.create_task(dispatcher.serve_forever())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    dispatcher._consumer.commit.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "same_partition",
    [False, True, None],
    ids=["lost_partition", "same_partition_new_epoch", "bounded_transient_retries"],
)
async def test_lost_assignment_or_same_partition_new_epoch_recreates_from_committed_offsets(
    monkeypatch, same_partition
):
    from eveys_ocpp.webhooks import dispatcher as module

    d = WebhookDispatcher(Settings(_env_file=None))
    old_tp, retained_tp = TopicPartition("tx.stopped", 0), TopicPartition("tx.stopped", 1)
    calls, consumers = [], []
    monkeypatch.setattr(module, "_COMMIT_RETRY_LIMIT", 2)

    class Consumer:
        def __init__(self, *topics, **kwargs):
            self.index = len(consumers)
            self.listener = None
            self.owned = (
                {old_tp, retained_tp}
                if same_partition is not False or not self.index
                else {retained_tp}
            )
            self.getmany_calls = 0
            self.commits = []
            self.stopped = False
            consumers.append(self)

        def subscribe(self, topics=(), listener=None):
            self.listener = listener

        async def start(self):
            if self.listener:
                await self.listener.on_partitions_assigned(self.owned)

        async def stop(self):
            self.stopped = True

        def assignment(self):
            return set(self.owned)

        async def getmany(self, **kwargs):
            self.getmany_calls += 1
            return {
                tp: [SimpleNamespace(topic=tp.topic, offset=10 if tp == old_tp else 20)]
                for tp in (old_tp, retained_tp)
                if tp in self.owned
            }

        async def commit(self, offsets=None):
            self.commits.append(offsets)
            if self.index == 0 and same_partition is None:
                raise RuntimeError("transient broker failure")
            if self.index == 0 and len(self.commits) == 1:
                if self.listener:
                    await self.listener.on_partitions_revoked(self.owned)
                if not same_partition:
                    self.owned = {retained_tp}
                if self.listener:
                    await self.listener.on_partitions_assigned(self.owned)
                raise IllegalStateError("partition ownership changed")
            if self.index == 0 and not same_partition:
                raise IllegalStateError("old partition not assigned")
            if self.index == 0 or len(self.commits) == len(self.owned):
                d._shutdown.set()

    monkeypatch.setattr(module, "AIOKafkaConsumer", Consumer)
    await d.start()

    async def deliver(record):
        calls.append(record.offset)

    d._deliver_one = deliver
    d._sample_consumer_lag = AsyncMock()
    try:
        await asyncio.wait_for(d.serve_forever(), timeout=2)
        assert len(consumers) == 2
        assert consumers[0].stopped
        assert consumers[0].getmany_calls == consumers[1].getmany_calls == 1
        assert [{tp: off.offset for tp, off in ack.items()} for ack in consumers[1].commits] == (
            [{old_tp: 11}, {retained_tp: 21}]
            if same_partition is not False
            else [{retained_tp: 21}]
        )
        assert calls == ([10, 10, 20] if same_partition is not False else [10, 20])
        if same_partition is None:
            assert len(consumers[0].commits) == 2
            assert consumers[0].commits[0] == consumers[0].commits[1]
    finally:
        await d.stop()


@pytest.mark.asyncio
async def test_max_poll_rebalance_interrupts_unacked_delivery_and_replays_stable_record(
    monkeypatch,
):
    from eveys_ocpp.webhooks import dispatcher as module

    d = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    entered, cancelled = asyncio.Event(), asyncio.Event()
    consumers, delivered = [], []
    body = events_pb2.EventEnvelope(
        event_id=str(uuid4()), cp_id="CP_TEST", tx_stopped=events_pb2.TxStopped(transaction_id=4823)
    ).SerializeToString()
    record = SimpleNamespace(topic=tp.topic, offset=5, value=body)
    signals = []

    class Consumer:
        def __init__(self, *topics, **kwargs):
            self.index = len(consumers)
            self.listener = None
            self.commits = []
            self.polls = 0
            consumers.append(self)

        def subscribe(self, topics=(), listener=None):
            self.listener = listener

        async def start(self):
            if self.listener:
                await self.listener.on_partitions_assigned({tp})

        async def stop(self):
            pass

        def assignment(self):
            return {tp}

        async def getmany(self, **kwargs):
            self.polls += 1
            if self.index == 0:

                async def max_poll_elapsed():
                    await entered.wait()
                    if self.listener:
                        await self.listener.on_partitions_revoked({tp})
                        await self.listener.on_partitions_assigned({tp})

                signals.append(asyncio.create_task(max_poll_elapsed()))
            return {tp: [record]}

        async def commit(self, offsets):
            self.commits.append(offsets)
            d._shutdown.set()

    monkeypatch.setattr(module, "AIOKafkaConsumer", Consumer)
    await d.start()

    async def deliver(value):
        if len(consumers) == 1:
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
        delivered.append(value.value)

    d._deliver_one = deliver
    d._sample_consumer_lag = AsyncMock()
    try:
        await asyncio.wait_for(d.serve_forever(), timeout=2)
        assert cancelled.is_set()
        assert len(consumers) == 2
        assert consumers[0].commits == []
        assert consumers[0].polls == consumers[1].polls == 1
        assert consumers[1].commits[0][tp].offset == 6
        assert delivered == [body]
    finally:
        for signal in signals:
            signal.cancel()
        await asyncio.gather(*signals, return_exceptions=True)
        await d.stop()


@pytest.mark.asyncio
async def test_successful_prefix_survives_max_poll_without_starving_next_pending_record(
    monkeypatch,
):
    from eveys_ocpp.webhooks import dispatcher as module

    d = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    records = [
        SimpleNamespace(topic=tp.topic, offset=i, value=f"stable-stop-{i}".encode()) for i in (5, 6)
    ]
    committed, consumers, delivered, signals = {}, [], [], []
    entered = asyncio.Event()

    class Consumer:
        def __init__(self, *topics, **kwargs):
            self.index = len(consumers)
            self.listener = None
            self.commits = []
            self.polled_offsets = []
            consumers.append(self)

        def subscribe(self, topics=(), listener=None):
            self.listener = listener

        async def start(self):
            if self.listener:
                await self.listener.on_partitions_assigned({tp})

        async def stop(self):
            pass

        def assignment(self):
            return {tp}

        async def getmany(self, **kwargs):
            visible = [r for r in records if r.offset >= committed.get(tp, 5)]
            self.polled_offsets = [r.offset for r in visible]
            if self.index == 0:

                async def rebalance():
                    await entered.wait()
                    if self.listener:
                        await self.listener.on_partitions_revoked({tp})
                        await self.listener.on_partitions_assigned({tp})

                signals.append(asyncio.create_task(rebalance()))
            return {tp: visible}

        async def commit(self, offsets):
            self.commits.append(offsets)
            committed.update({key: value.offset for key, value in offsets.items()})
            if self.index > 0 and offsets[tp].offset == 7:
                d._shutdown.set()

    monkeypatch.setattr(module, "AIOKafkaConsumer", Consumer)
    await d.start()

    async def deliver(record):
        if record.offset == 6 and len(consumers) == 1:
            entered.set()
            await asyncio.Event().wait()
        delivered.append(record.value)

    d._deliver_one = deliver
    d._sample_consumer_lag = AsyncMock()
    try:
        await asyncio.wait_for(d.serve_forever(), timeout=2)
        assert consumers[0].commits[0][tp].offset == 6
        assert consumers[1].polled_offsets == [6]
        assert delivered == [r.value for r in records]
        assert committed[tp] == 7
    finally:
        for signal in signals:
            signal.cancel()
        await asyncio.gather(*signals, return_exceptions=True)
        await d.stop()


@pytest.mark.asyncio
async def test_thousand_record_batch_progresses_monotonically_across_repeated_max_poll_rejoins(
    monkeypatch,
):
    from eveys_ocpp.webhooks import dispatcher as module

    d = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    records = [
        SimpleNamespace(topic=tp.topic, offset=i, value=f"stable-event-{i}".encode())
        for i in range(1000)
    ]
    consumers, delivered, broker_acks = [], [], []
    committed = 0

    class Consumer:
        def __init__(self, *topics, **kwargs):
            self.index = len(consumers)
            self.listener = None
            self.start_offset = None
            consumers.append(self)

        def subscribe(self, topics=(), listener=None):
            self.listener = listener

        async def start(self):
            await self.listener.on_partitions_assigned({tp})

        async def stop(self):
            pass

        def assignment(self):
            return {tp}

        async def getmany(self, **kwargs):
            self.start_offset = committed
            return {tp: records[committed:]}

        async def commit(self, offsets):
            nonlocal committed
            assert offsets[tp].offset == committed + 1
            committed = offsets[tp].offset
            broker_acks.append(committed)
            if committed == 1000:
                d._shutdown.set()

    monkeypatch.setattr(module, "AIOKafkaConsumer", Consumer)
    await d.start()

    async def deliver(record):
        current = consumers[-1]
        if current.index < 3 and record.offset == 300 * (current.index + 1):
            await current.listener.on_partitions_revoked({tp})
            await current.listener.on_partitions_assigned({tp})
            await asyncio.Event().wait()
        delivered.append(record.value)

    d._deliver_one = deliver
    d._sample_consumer_lag = AsyncMock()
    try:
        await asyncio.wait_for(d.serve_forever(), timeout=10)
        assert [c.start_offset for c in consumers] == [0, 300, 600, 900]
        assert delivered == [r.value for r in records]
        assert broker_acks == list(range(1, 1001))
    finally:
        await d.stop()


@pytest.mark.asyncio
async def test_cancelled_partial_prefix_commit_does_not_assume_the_pending_ack():
    d = WebhookDispatcher(Settings(_env_file=None))
    tp = TopicPartition("tx.stopped", 0)
    records = [SimpleNamespace(topic=tp.topic, offset=i) for i in (5, 6)]
    entered = asyncio.Event()
    acknowledged, attempted, delivered = [], [], []

    async def commit(offsets):
        attempted.append(offsets[tp].offset)
        if offsets[tp].offset == 7:
            entered.set()
            await asyncio.Event().wait()
        acknowledged.append(offsets[tp].offset)

    async def deliver(record):
        delivered.append(record.offset)

    d._consumer = SimpleNamespace(
        getmany=AsyncMock(return_value={tp: records}), commit=commit, assignment=lambda: {tp}
    )
    d._http = object()
    d._deliver_one = deliver
    task = asyncio.create_task(d.serve_forever())
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert acknowledged == [6]
    assert attempted == [6, 7]
    assert delivered == [5, 6]
    d._consumer.getmany.assert_awaited_once()


@pytest.mark.asyncio
async def test_consumer_close_timeout_does_not_skip_http_cleanup(monkeypatch):
    from eveys_ocpp.webhooks import dispatcher as module

    monkeypatch.setattr(module, "_CONSUMER_OPERATION_TIMEOUT_SECONDS", 0.01)
    d = WebhookDispatcher(Settings(_env_file=None))

    async def hang():
        await asyncio.Event().wait()

    d._consumer = SimpleNamespace(stop=hang, assignment=lambda: set())
    http = SimpleNamespace(aclose=AsyncMock())
    d._http = http
    await asyncio.wait_for(d.stop(), timeout=0.1)
    assert d._consumer is None
    assert d._http is None
    http.aclose.assert_awaited_once()
