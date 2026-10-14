"""Webhook dispatcher: Kafka → HMAC-signed HTTP POST (E3-9).

Long-lived background task. Tails the four event topics the gateway
publishes on, fans each event out to the configured webhook URL for
its event type, signs with HMAC-SHA-256, retries failures with
exponential backoff per the contract in
`docs/integration/03-webhooks.md`.

Distinct Kafka consumer group from the ClickHouse ingestor so the two
consumers run independently — webhook delivery falling behind doesn't
back-pressure ClickHouse and vice-versa.

Per-event toggles in `Settings` (`webhook_enable_*`) decide which
topics actually leave the gateway as webhooks. `cp.meter` is off by
default (high volume; Kafka is the right channel).
"""

from __future__ import annotations

import asyncio
import json
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx
from aiokafka import AIOKafkaConsumer, ConsumerRebalanceListener
from aiokafka.errors import CommitFailedError, IllegalStateError
from aiokafka.structs import OffsetAndMetadata

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp._ocpp_enums import ocpp_string_for
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.repositories import insert_webhook_backlog
from eveys_ocpp.runtime_overrides import get_override
from eveys_ocpp.webhooks.signer import compute_signature

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# Backoff schedule per docs/integration/03-webhooks.md § Delivery
# semantics. Indexed by attempt number (0 = first attempt; we sleep
# this many seconds BEFORE the (n+1)th attempt).
_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 5.0, 15.0, 30.0, 60.0)
_COMMIT_RETRY_LIMIT = 5
_CONSUMER_OPERATION_TIMEOUT_SECONDS = 5.0
# Leave room for a durable SQL ACK without holding one Kafka record for
# the default 300 s max-poll interval. These are total wall-clock budgets,
# not HTTPX's per-phase timeouts. Cancellation never establishes an ACK.
_DELIVERY_HTTP_BUDGET_SECONDS = 55.0
_BACKLOG_COMMIT_TIMEOUT_SECONDS = 5.0


def _delivery_now() -> float:
    return time.monotonic()


class _OwnershipChanged(RuntimeError):
    pass


# aiokafka does not publish typing metadata for this callback base class.
class _RebalanceListener(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(self, dispatcher: WebhookDispatcher) -> None:
        self._dispatcher = dispatcher

    async def on_partitions_revoked(self, revoked: Any) -> None:
        self._dispatcher._assignment_changed(revoked)

    async def on_partitions_assigned(self, assigned: Any) -> None:
        self._dispatcher._assignment_changed(assigned)


class WebhookDispatcher:
    """Tail event topics, sign, deliver. One instance per process.

    Optional ``session_factory`` unlocks the durable backlog: when
    the in-loop retry budget is exhausted the dispatcher inserts a
    row into ``webhook_delivery_backlog`` for
    :class:`WebhookBacklogDrainer` to pick up. Legacy callers
    (tests, tools that don't touch Postgres) can pass ``None`` and
    retain the Kafka batch on failed HTTP delivery until a durable ACK
    becomes possible. Missing or disabled backlog never permits a drop.
    """

    def __init__(
        self,
        settings: Settings,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
    ) -> None:
        self._settings = settings
        self._session_factory = session_factory
        self._consumer: AIOKafkaConsumer | None = None
        self._http: httpx.AsyncClient | None = None
        self._shutdown = asyncio.Event()
        self._assignment_epoch = 0
        self._assignment_changed_event = asyncio.Event()
        self._topics: tuple[str, ...] = ()

    def _assignment_changed(self, partitions: Any) -> None:
        self._assignment_epoch += 1
        self._assignment_changed_event.set()
        for tp in partitions:
            metrics_registry.WEBHOOK_CONSUMER_LAG_MESSAGES.labels(
                topic=tp.topic, partition=str(tp.partition)
            ).set(-1)

    async def _open_consumer(self) -> None:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._settings.kafka_brokers,
            group_id=self._settings.webhook_consumer_group,
            client_id="eveys-ocpp-webhook-dispatcher",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        consumer.subscribe(topics=self._topics, listener=_RebalanceListener(self))
        try:
            async with asyncio.timeout(_CONSUMER_OPERATION_TIMEOUT_SECONDS):
                await consumer.start()
        except BaseException:
            with suppress(Exception):
                async with asyncio.timeout(_CONSUMER_OPERATION_TIMEOUT_SECONDS):
                    await consumer.stop()
            raise
        self._consumer = consumer

    async def _recreate_consumer(self) -> None:
        """Release stale ownership; rejoin from broker committed offsets.

        No revoked offset is committed, no seek/offset reset is performed.
        Completed HTTP requests may replay with their stable event UUID.
        """
        old, self._consumer = self._consumer, None
        self._assignment_changed(old.assignment() if old is not None else ())
        if old is not None:
            with suppress(Exception):
                async with asyncio.timeout(_CONSUMER_OPERATION_TIMEOUT_SECONDS):
                    await old.stop()
        while not self._shutdown.is_set():
            try:
                await self._open_consumer()
                return
            except Exception as exc:
                log.warning("webhook_dispatcher.rejoin_failed", error_class=type(exc).__name__)
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._shutdown.wait(), timeout=1.0)

    def _owns(self, consumer: AIOKafkaConsumer, epoch: int, partitions: Any) -> bool:
        return self._assignment_epoch == epoch and set(partitions).issubset(consumer.assignment())

    async def _while_owned(
        self,
        operation: Any,
        *,
        consumer: AIOKafkaConsumer,
        epoch: int,
        partitions: Any,
        timeout: float | None = None,
    ) -> Any:
        task = asyncio.ensure_future(operation)
        changed = asyncio.create_task(self._assignment_changed_event.wait())
        try:
            if not self._owns(consumer, epoch, partitions):
                raise _OwnershipChanged()
            async with asyncio.timeout(timeout):
                done, _ = await asyncio.wait((task, changed), return_when=asyncio.FIRST_COMPLETED)
                if changed in done or not self._owns(consumer, epoch, partitions):
                    raise _OwnershipChanged()
                return await task
        finally:
            for pending in (task, changed):
                if not pending.done():
                    pending.cancel()
            await asyncio.gather(task, changed, return_exceptions=True)

    async def start(self) -> None:
        """Open the Kafka consumer and the HTTP client. Idempotent
        only in the sense that calling it twice is a programming
        error; tests reuse the same instance with the same start →
        run → stop lifecycle as the ingestor."""
        topics = self._enabled_topics()
        if not topics:
            log.info("webhook_dispatcher.no_enabled_events")
            return

        self._topics = tuple(topics)
        await self._open_consumer()

        if not self._settings.outbound_tls_verify:
            # Symmetric warning to the one BackendHTTPClient emits.
            # Both outbound legs honour the same flag; logging at each
            # site means an operator scanning for "tls_verify_disabled"
            # finds the webhook dispatcher's own line too instead of
            # having to know it shares config with the backend client.
            log.warning(
                "webhook.tls_verify_disabled",
                detail=(
                    "EVEYS_OCPP_OUTBOUND_TLS_VERIFY=False — accepting "
                    "any TLS cert on the webhook leg. Acceptable for "
                    "local dev; never in production."
                ),
            )
        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(self._settings.webhook_request_timeout_seconds),
            verify=self._settings.outbound_tls_verify,
        )

        log.info(
            "webhook_dispatcher.started",
            kafka_brokers=self._settings.kafka_brokers,
            topics=list(topics),
            base_url=self._settings.webhook_base_url,
            max_attempts=self._settings.webhook_max_attempts,
        )

    async def stop(self) -> None:
        self._shutdown.set()
        consumer, self._consumer = self._consumer, None
        http, self._http = self._http, None
        self._assignment_changed(consumer.assignment() if consumer is not None else ())
        try:
            if consumer is not None:
                async with asyncio.timeout(_CONSUMER_OPERATION_TIMEOUT_SECONDS):
                    await consumer.stop()
        except Exception as exc:
            log.warning("webhook_dispatcher.consumer_close_failed", error_class=type(exc).__name__)
        finally:
            if http is not None:
                try:
                    async with asyncio.timeout(_CONSUMER_OPERATION_TIMEOUT_SECONDS):
                        await http.aclose()
                except Exception as exc:
                    log.warning(
                        "webhook_dispatcher.http_close_failed", error_class=type(exc).__name__
                    )
        log.info("webhook_dispatcher.stopped")

    async def serve_forever(self) -> None:
        """Main loop: poll Kafka, deliver each record, commit offsets.

        Exits on `stop()`. Failed delivery/commit retains the exact batch;
        no subsequent poll may move the failure boundary. Failures during
        Kafka consumer ops (broker dropping, etc.) propagate so the
        TaskGroup can decide how to handle them."""
        if self._consumer is None or self._http is None:
            # `start()` returned early because no events are enabled.
            # Stay alive in case the TaskGroup is waiting on us, but do
            # nothing.
            await self._shutdown.wait()
            return

        try:
            while not self._shutdown.is_set():
                consumer = self._consumer
                if consumer is None:
                    await self._recreate_consumer()
                    continue
                # 1 s poll keeps shutdown latency low without burning CPU.
                batch = await consumer.getmany(timeout_ms=1000, max_records=1000)
                if not batch:
                    continue
                epoch = self._assignment_epoch
                self._assignment_changed_event.clear()
                # Keep the unresolved batch while committing only each
                # delivered prefix. Rejoins resume broker-ACKed progress
                # instead of replaying a slow successful prefix forever.
                pending = [(tp, record) for tp, records in batch.items() for record in records]
                cursor = 0
                delivered = False
                commit_failures = 0
                rejoin = False
                while cursor < len(pending) and not self._shutdown.is_set():
                    try:
                        if not self._owns(consumer, epoch, batch):
                            raise _OwnershipChanged()
                        tp, record = pending[cursor]
                        if not delivered:
                            await self._while_owned(
                                self._deliver_one(record),
                                consumer=consumer,
                                epoch=epoch,
                                partitions=batch,
                            )
                            delivered = True
                        # Explicit partition offsets cannot acknowledge a
                        # later record or an unrelated pending partition.
                        offsets = {tp: OffsetAndMetadata(record.offset + 1, "")}
                        await self._while_owned(
                            consumer.commit(offsets),
                            consumer=consumer,
                            epoch=epoch,
                            partitions=batch,
                            timeout=_CONSUMER_OPERATION_TIMEOUT_SECONDS,
                        )
                    except (_OwnershipChanged, IllegalStateError, CommitFailedError):
                        rejoin = True
                        break
                    except Exception as exc:
                        # Do not repeat a completed delivery for a transient
                        # commit failure, or assume its ACK until it returns.
                        if delivered:
                            commit_failures += 1
                            if commit_failures >= _COMMIT_RETRY_LIMIT:
                                rejoin = True
                                break
                        log.warning(
                            "webhook_dispatcher.batch_pending", error_class=type(exc).__name__
                        )
                        with suppress(TimeoutError):
                            await asyncio.wait_for(self._shutdown.wait(), timeout=1.0)
                    else:
                        cursor += 1
                        delivered = False
                        commit_failures = 0
                if self._shutdown.is_set():
                    break
                if rejoin:
                    await self._recreate_consumer()
                    continue
                # Sample the per-partition lag right after committing.
                # Cheap (one Kafka API call per assigned partition) and
                # only fires when there was traffic this iteration —
                # idle partitions don't poll the broker for nothing.
                try:
                    await self._while_owned(
                        self._sample_consumer_lag(consumer),
                        consumer=consumer,
                        epoch=epoch,
                        partitions=batch,
                        timeout=_CONSUMER_OPERATION_TIMEOUT_SECONDS,
                    )
                except Exception as exc:
                    log.debug(
                        "webhook_dispatcher.observation_interrupted", error_class=type(exc).__name__
                    )
        except asyncio.CancelledError:
            log.info("webhook_dispatcher.cancelled")
            raise

    async def _sample_consumer_lag(self, consumer: AIOKafkaConsumer) -> None:
        """Update WEBHOOK_CONSUMER_LAG_MESSAGES{topic,partition} for
        every assigned partition: lag = end_offset - position.

        Errors are swallowed — the lag gauge is best-effort observability
        and a transient broker hiccup must not crash the delivery loop.
        """
        try:
            assigned = consumer.assignment()
            if not assigned:
                return
            end_offsets = await consumer.end_offsets(list(assigned))
        except Exception as exc:
            log.debug("webhook_dispatcher.lag_sample_failed", error=str(exc))
            return
        for tp, end in end_offsets.items():
            try:
                position = await consumer.position(tp)
            except Exception:
                continue
            lag = max(0, int(end) - int(position))
            metrics_registry.WEBHOOK_CONSUMER_LAG_MESSAGES.labels(
                topic=tp.topic, partition=str(tp.partition)
            ).set(lag)

    # ---- per-record delivery ----------------------------------------------

    async def _deliver_one(self, record: ConsumerRecord) -> None:
        """Decode a Kafka record, build the webhook body, deliver
        with retry. Return only after HTTP 2xx or committed backlog.
        Malformed/disabled events raise and retain the caller's batch."""
        envelope = events_pb2.EventEnvelope()
        try:
            envelope.ParseFromString(record.value)
        except Exception as exc:
            log.warning(
                "webhook_dispatcher.parse_failed",
                topic=record.topic,
                offset=record.offset,
                error=str(exc),
            )
            raise RuntimeError("webhook envelope could not be decoded") from exc

        try:
            UUID(envelope.event_id)
        except (ValueError, TypeError) as exc:
            raise RuntimeError("webhook envelope has no stable event identity") from exc

        body = self._build_body(envelope)
        if body is None:
            raise RuntimeError("webhook event unavailable or disabled")

        url = self._url_for(envelope)
        if url is None:
            raise RuntimeError("webhook destination unavailable or disabled")

        body_bytes = json.dumps(body, separators=(",", ":")).encode("utf-8")
        # E5-7: webhook_secret is a SecretStr; unwrap once at the
        # signing boundary. compute_signature takes the raw key and
        # never logs it.
        signature = compute_signature(body_bytes, self._settings.webhook_secret.get_secret_value())

        await self._post_with_retry(
            url=url,
            body_bytes=body_bytes,
            signature=signature,
            event_id=envelope.event_id,
            event_type=body["data"]["event_type"],
        )

    async def _post_with_retry(
        self,
        *,
        url: str,
        body_bytes: bytes,
        signature: str,
        event_id: str,
        event_type: str,
    ) -> None:
        """At most 55 s HTTP/backoff plus 5 s for committed backlog.

        Return only for HTTP 2xx or a completed SQL commit. External
        cancellation/ownership loss cannot turn a pending record into ACK.
        """
        deadline = _delivery_now() + _DELIVERY_HTTP_BUDGET_SECONDS
        try:
            async with asyncio.timeout(_DELIVERY_HTTP_BUDGET_SECONDS):
                if await self._post_attempts(
                    url=url,
                    body_bytes=body_bytes,
                    signature=signature,
                    event_id=event_id,
                    event_type=event_type,
                    deadline=deadline,
                ):
                    return
        except TimeoutError:
            log.warning(
                "webhook.delivery_budget_exhausted", event_id=event_id, event_type=event_type
            )

        log.error(
            "webhook.delivery_failed",
            url=url,
            event_id=event_id,
            event_type=event_type,
            max_attempts=self._settings.webhook_max_attempts,
        )
        metrics_registry.WEBHOOK_DELIVERIES_TOTAL.labels(
            event_type=event_type, outcome="failed"
        ).inc()
        try:
            async with asyncio.timeout(_BACKLOG_COMMIT_TIMEOUT_SECONDS):
                await self._enqueue_backlog(
                    url=url,
                    body_bytes=body_bytes,
                    signature=signature,
                    event_id=event_id,
                    event_type=event_type,
                )
        except TimeoutError as exc:
            raise RuntimeError(
                "webhook backlog commit deadline exceeded; delivery remains pending"
            ) from exc

    async def _post_attempts(
        self,
        *,
        url: str,
        body_bytes: bytes,
        signature: str,
        event_id: str,
        event_type: str,
        deadline: float,
    ) -> bool:
        assert self._http is not None  # narrowed by start()

        max_attempts = self._settings.webhook_max_attempts
        for attempt in range(1, max_attempts + 1):
            remaining = deadline - _delivery_now()
            if remaining <= 0:
                break
            metrics_registry.WEBHOOK_ATTEMPTS_TOTAL.labels(event_type=event_type).inc()
            headers = {
                "Content-Type": "application/json",
                "X-Eveys-Signature": signature,
                "X-Eveys-Event-Id": event_id,
                "X-Eveys-Event-Type": event_type,
                "X-Eveys-Delivered-At": datetime.now(UTC).isoformat(),
                "X-Eveys-Attempt": str(attempt),
            }
            attempt_started = time.perf_counter()
            try:
                async with asyncio.timeout(
                    min(self._settings.webhook_request_timeout_seconds, remaining)
                ):
                    response = await self._http.post(url, content=body_bytes, headers=headers)
            except (TimeoutError, httpx.TimeoutException, httpx.NetworkError) as exc:
                metrics_registry.WEBHOOK_DELIVERY_LATENCY_SECONDS.labels(
                    event_type=event_type
                ).observe(time.perf_counter() - attempt_started)
                log.warning(
                    "webhook.delivery_attempt_failed",
                    url=url,
                    event_id=event_id,
                    event_type=event_type,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt >= max_attempts:
                    break
                delay = _BACKOFF_SECONDS[min(attempt - 1, len(_BACKOFF_SECONDS) - 1)]
                if delay >= deadline - _delivery_now():
                    break
                await asyncio.sleep(delay)
                continue

            metrics_registry.WEBHOOK_DELIVERY_LATENCY_SECONDS.labels(event_type=event_type).observe(
                time.perf_counter() - attempt_started
            )

            if 200 <= response.status_code < 300:
                log.info(
                    "webhook.delivered",
                    url=url,
                    event_id=event_id,
                    event_type=event_type,
                    attempt=attempt,
                    status=response.status_code,
                )
                metrics_registry.WEBHOOK_DELIVERIES_TOTAL.labels(
                    event_type=event_type, outcome="delivered"
                ).inc()
                return True

            # Non-2xx: everything is retryable. The customer contract
            # is "only 2xx = accepted"; a 4xx from the backend during a
            # deploy (401 during token rotation, 502 during load-balancer
            # flip) is still worth retrying rather than dropping the
            # envelope forever. Retention aging in the backlog is now
            # the ONLY path to `dead=true`.
            log.warning(
                "webhook.delivery_attempt_failed",
                url=url,
                event_id=event_id,
                event_type=event_type,
                attempt=attempt,
                status=response.status_code,
                body=response.text[:200],
            )
            if attempt >= max_attempts:
                break
            delay = _BACKOFF_SECONDS[min(attempt - 1, len(_BACKOFF_SECONDS) - 1)]
            if delay >= deadline - _delivery_now():
                break
            await asyncio.sleep(delay)
            continue
        return False

    async def _enqueue_backlog(
        self,
        *,
        url: str,
        body_bytes: bytes,
        signature: str,
        event_id: str,
        event_type: str,
    ) -> None:
        """Persist an exhausted envelope into
        ``webhook_delivery_backlog`` for the drainer to keep
        retrying.

        Missing/disabled backlog, invalid identity, or commit failure
        raises. The consumer must retain its batch and not commit offsets.

        The row's ``next_attempt_at`` is set to
        ``now + backlog_poll_seconds`` so the drainer picks it up on
        its next cycle instead of racing against the current one.
        A parse failure on ``event_id`` is retained for investigation.
        """
        if not self._settings.webhook_backlog_enabled:
            raise RuntimeError("webhook backlog is disabled; delivery remains pending")
        if self._session_factory is None:
            log.warning(
                "webhook.backlog_no_session_factory",
                event_id=event_id,
                event_type=event_type,
                detail=(
                    "webhook_backlog_enabled=True but the dispatcher was "
                    "constructed without a session_factory; envelope "
                    "pending. Wire the factory in __main__.py."
                ),
            )
            raise RuntimeError("webhook backlog has no session factory")
        try:
            event_uuid = UUID(event_id)
        except (TypeError, ValueError) as exc:
            log.warning(
                "webhook.backlog_bad_event_id",
                event_id=event_id,
                event_type=event_type,
                error=str(exc),
            )
            raise RuntimeError("webhook backlog has no stable event identity") from exc
        next_attempt = datetime.now(UTC) + timedelta(
            seconds=self._settings.webhook_backlog_poll_seconds
        )
        try:
            async with self._session_factory() as session:
                await insert_webhook_backlog(
                    session,
                    event_id=event_uuid,
                    event_type=event_type,
                    url=url,
                    body=body_bytes,
                    signature=signature,
                    next_attempt_at=next_attempt,
                )
                await session.commit()
        except Exception as exc:
            log.exception(
                "webhook.backlog_insert_failed",
                event_id=event_id,
                event_type=event_type,
                error=str(exc),
            )
            raise RuntimeError("webhook backlog commit failed") from exc
        metrics_registry.WEBHOOK_BACKLOG_ENQUEUED_TOTAL.labels(event_type=event_type).inc()
        log.info(
            "webhook.backlog_enqueued",
            event_id=event_id,
            event_type=event_type,
            next_attempt_at=next_attempt.isoformat(),
        )

    # ---- envelope → wire body ---------------------------------------------

    def _enabled(self, name: str, default: bool) -> bool:
        return bool(get_override(name, default))

    def _url(self, name: str, default: str) -> str:
        value = get_override(name, default)
        return value if isinstance(value, str) else default

    def _base_url(self) -> str:
        return self._url("webhook_base_url", self._settings.webhook_base_url)

    def _build_body(self, envelope: events_pb2.EventEnvelope) -> dict[str, Any] | None:
        """Translate one EventEnvelope to the webhook JSON body, per
        `docs/integration/03-webhooks.md` § Event catalog. Returns
        None when the event type isn't enabled (or is unknown).

        Events delivered today: cp.boot, cp.online, cp.offline,
        cp.heartbeat, cp.status_changed, cp.firmware_status_changed,
        cp.diagnostics_status_changed, tx.started, tx.stopped.
        cp.meter is implemented but off-by-default (high volume —
        Kafka subscription is the right channel; see ADR-0027).
        """
        kind = envelope.WhichOneof("payload")
        if kind == "cp_connected" and self._enabled(
            "webhook_enable_cp_online", self._settings.webhook_enable_cp_online
        ):
            p = envelope.cp_connected
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.online",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "subprotocol": p.subprotocol,
                "pod_id": p.pod_id,
            }
            return _envelope(data)

        if kind == "cp_disconnected" and self._enabled(
            "webhook_enable_cp_offline", self._settings.webhook_enable_cp_offline
        ):
            p_off = envelope.cp_disconnected
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.offline",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "pod_id": p_off.pod_id,
                "reason": p_off.reason,
            }
            return _envelope(data)

        if kind == "cp_heartbeat" and self._enabled(
            "webhook_enable_cp_heartbeat", self._settings.webhook_enable_cp_heartbeat
        ):
            p_hb = envelope.cp_heartbeat
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.heartbeat",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "pod_id": p_hb.pod_id,
            }
            return _envelope(data)

        if kind == "cp_boot" and self._enabled(
            "webhook_enable_cp_boot", self._settings.webhook_enable_cp_boot
        ):
            p = envelope.cp_boot
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.boot",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "vendor": p.vendor,
                "model": p.model,
                "firmware_version": p.firmware_version,
                "serial_number": p.serial_number,
                "registration_status": _boot_status_name(p.status),
            }
            return _envelope(data)

        if kind == "cp_status" and self._enabled(
            "webhook_enable_cp_status", self._settings.webhook_enable_cp_status
        ):
            p = envelope.cp_status
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.status_changed",
                "occurred_at": envelope.occurred_at,
                "charger_reported_at": p.charger_reported_at,
                "cp_id": envelope.cp_id,
                "connector_id": p.connector_id,
                "status": p.status,
                "error_code": p.error_code,
                "info": p.info,
                "vendor_id": p.vendor_id,
                "vendor_error_code": p.vendor_error_code,
            }
            return _envelope(data)

        if kind == "tx_started" and self._enabled(
            "webhook_enable_tx_started", self._settings.webhook_enable_tx_started
        ):
            p = envelope.tx_started
            data = {
                "event_id": envelope.event_id,
                "event_type": "tx.started",
                "occurred_at": envelope.occurred_at,
                "charger_reported_at": p.charger_reported_at,
                "cp_id": envelope.cp_id,
                "connector_id": p.connector_id,
                "transaction_id": p.transaction_id,
                "id_tag": p.id_tag,
                "meter_start_wh": p.meter_start_wh,
            }
            return _envelope(data)

        if kind == "tx_stopped" and self._enabled(
            "webhook_enable_tx_stopped", self._settings.webhook_enable_tx_stopped
        ):
            p = envelope.tx_stopped
            data = {
                "event_id": envelope.event_id,
                "event_type": "tx.stopped",
                "occurred_at": envelope.occurred_at,
                "charger_reported_at": p.charger_reported_at,
                "cp_id": envelope.cp_id,
                "transaction_id": p.transaction_id,
                "id_tag": p.id_tag,
                "meter_stop_wh": p.meter_stop_wh,
                "consumed_wh": p.consumed_wh,
                "stop_reason": p.stop_reason,
            }
            if p.connector_id > 0:
                data["connector_id"] = p.connector_id
            if p.stopped_received_at:
                data["stopped_received_at"] = p.stopped_received_at
            return _envelope(data)

        if (
            kind == "cp_firmware_status_changed"
            and self._settings.webhook_enable_cp_firmware_status
        ):
            p_fw = envelope.cp_firmware_status_changed
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.firmware_status_changed",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "status": p_fw.status,
            }
            return _envelope(data)

        if (
            kind == "cp_diagnostics_status_changed"
            and self._settings.webhook_enable_cp_diagnostics_status
        ):
            p_dg = envelope.cp_diagnostics_status_changed
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.diagnostics_status_changed",
                "occurred_at": envelope.occurred_at,
                "cp_id": envelope.cp_id,
                "status": p_dg.status,
            }
            return _envelope(data)

        if kind == "cp_meter" and self._enabled(
            "webhook_enable_cp_meter", self._settings.webhook_enable_cp_meter
        ):
            p_meter = envelope.cp_meter
            data = {
                "event_id": envelope.event_id,
                "event_type": "cp.meter_values",
                "occurred_at": envelope.occurred_at,
                "charger_reported_at": p_meter.charger_reported_at,
                "cp_id": envelope.cp_id,
                "connector_id": p_meter.connector_id,
                "transaction_id": p_meter.transaction_id,
                "sampled_values": [_sampled_value_to_ocpp(sv) for sv in p_meter.sampled_values],
            }
            return _envelope(data)

        # Unknown payloads: silently skip.
        return None

    def _url_for(self, envelope: events_pb2.EventEnvelope) -> str | None:
        """URL for this envelope's event type, or None if disabled."""
        kind = envelope.WhichOneof("payload")
        s = self._settings
        if kind == "cp_connected" and self._enabled(
            "webhook_enable_cp_online", s.webhook_enable_cp_online
        ):
            return self._url("webhook_url_cp_online", s.webhook_url_cp_online) or (
                f"{self._base_url()}/cp-online"
            )
        if kind == "cp_disconnected" and self._enabled(
            "webhook_enable_cp_offline", s.webhook_enable_cp_offline
        ):
            return self._url("webhook_url_cp_offline", s.webhook_url_cp_offline) or (
                f"{self._base_url()}/cp-offline"
            )
        if kind == "cp_heartbeat" and self._enabled(
            "webhook_enable_cp_heartbeat", s.webhook_enable_cp_heartbeat
        ):
            return self._url("webhook_url_cp_heartbeat", s.webhook_url_cp_heartbeat) or (
                f"{self._base_url()}/cp-heartbeat"
            )
        if kind == "cp_boot" and self._enabled("webhook_enable_cp_boot", s.webhook_enable_cp_boot):
            return self._url("webhook_url_cp_boot", s.webhook_url_cp_boot) or (
                f"{self._base_url()}/cp-boot"
            )
        if kind == "cp_status" and self._enabled(
            "webhook_enable_cp_status", s.webhook_enable_cp_status
        ):
            return self._url("webhook_url_cp_status", s.webhook_url_cp_status) or (
                f"{self._base_url()}/cp-status-changed"
            )
        if kind == "cp_meter" and self._enabled(
            "webhook_enable_cp_meter", s.webhook_enable_cp_meter
        ):
            return self._url("webhook_url_cp_meter", s.webhook_url_cp_meter) or (
                f"{self._base_url()}/cp-meter"
            )
        if kind == "tx_started" and self._enabled(
            "webhook_enable_tx_started", s.webhook_enable_tx_started
        ):
            return self._url("webhook_url_tx_started", s.webhook_url_tx_started) or (
                f"{self._base_url()}/tx-started"
            )
        if kind == "tx_stopped" and self._enabled(
            "webhook_enable_tx_stopped", s.webhook_enable_tx_stopped
        ):
            return self._url("webhook_url_tx_stopped", s.webhook_url_tx_stopped) or (
                f"{self._base_url()}/tx-stopped"
            )
        if kind == "cp_firmware_status_changed" and self._enabled(
            "webhook_enable_cp_firmware_status", s.webhook_enable_cp_firmware_status
        ):
            return self._url(
                "webhook_url_cp_firmware_status", s.webhook_url_cp_firmware_status
            ) or (f"{self._base_url()}/cp-firmware-status-changed")
        if kind == "cp_diagnostics_status_changed" and self._enabled(
            "webhook_enable_cp_diagnostics_status", s.webhook_enable_cp_diagnostics_status
        ):
            return self._url(
                "webhook_url_cp_diagnostics_status", s.webhook_url_cp_diagnostics_status
            ) or (f"{self._base_url()}/cp-diagnostics-status-changed")
        return None

    def _enabled_topics(self) -> tuple[str, ...]:
        """Subset of the event topics whose webhook delivery is
        currently enabled. Lets us avoid subscribing the consumer to
        topics nobody listens for."""
        s = self._settings
        topics: list[str] = []
        if s.webhook_enable_cp_boot:
            topics.append(s.kafka_topic_cp_boot)
        if s.webhook_enable_cp_online:
            topics.append(s.kafka_topic_cp_connected)
        if s.webhook_enable_cp_offline:
            topics.append(s.kafka_topic_cp_disconnected)
        if s.webhook_enable_cp_heartbeat:
            topics.append(s.kafka_topic_cp_heartbeat)
        if s.webhook_enable_cp_status:
            topics.append(s.kafka_topic_cp_status)
        if s.webhook_enable_cp_meter:
            topics.append(s.kafka_topic_cp_meter)
        if s.webhook_enable_tx_started:
            topics.append(s.kafka_topic_tx_started)
        if s.webhook_enable_tx_stopped:
            topics.append(s.kafka_topic_tx_stopped)
        if s.webhook_enable_cp_firmware_status:
            topics.append(s.kafka_topic_cp_firmware_status)
        if s.webhook_enable_cp_diagnostics_status:
            topics.append(s.kafka_topic_cp_diagnostics_status)
        return tuple(topics)


def _envelope(data: dict[str, Any]) -> dict[str, Any]:
    """Wrap a per-event data dict in the standard backend envelope."""
    return {
        "success": True,
        "data": data,
        "message": data["event_type"],
    }


def _sampled_value_to_ocpp(sv: events_pb2.SampledValue) -> dict[str, Any]:
    """Serialize one CpMeter SampledValue to the OCPP-wire JSON shape the
    backend expects.

    The proto schema stores each dimension as an enum (`Measurand`,
    `Unit`, `Phase`, `Context`, `Format`, `Location`); the backend's
    `MeterValuesIngestJob` mirrors the legacy OCPP `MeterValues` listener
    which matches on OCPP wire strings (`"Power.Active.Import"`,
    `"Outlet"`, `"Wh"`, …). Translate via `_ocpp_enums.ocpp_string_for`
    so the round-trip stays consistent with the read path.

    `*_UNSPECIFIED` / unknown enum names round-trip to empty string rather
    than `None` so the legacy listener's `isset(...) ? : ""` pattern
    doesn't trip on missing keys.
    """

    def _name(value: int, enum_name: str) -> str:
        return events_pb2.DESCRIPTOR.enum_types_by_name[enum_name].values_by_number[value].name

    measurand = ocpp_string_for("measurand", _name(sv.measurand, "Measurand")) or ""
    unit = ocpp_string_for("unit", _name(sv.unit, "Unit")) or ""
    phase = ocpp_string_for("phase", _name(sv.phase, "Phase")) or ""
    context = ocpp_string_for("context", _name(sv.context, "Context")) or ""
    fmt = ocpp_string_for("format", _name(sv.format, "Format")) or ""
    location = ocpp_string_for("location", _name(sv.location, "Location")) or ""

    return {
        "value": sv.value,
        "measurand": measurand,
        "unit": unit,
        "phase": phase,
        "context": context,
        "format": fmt,
        "location": location,
    }


def _boot_status_name(status: int) -> str:
    """Proto enum int → spec string."""
    if status == events_pb2.CP_BOOT_STATUS_ACCEPTED:
        return "Accepted"
    if status == events_pb2.CP_BOOT_STATUS_PENDING:
        return "Pending"
    if status == events_pb2.CP_BOOT_STATUS_REJECTED:
        return "Rejected"
    return "Unspecified"
