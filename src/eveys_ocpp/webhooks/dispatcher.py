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
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import httpx
from aiokafka import AIOKafkaConsumer

from eveys_ocpp._generated.events.v1 import events_pb2
from eveys_ocpp._ocpp_enums import ocpp_string_for
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger
from eveys_ocpp.runtime_overrides import get_override
from eveys_ocpp.webhooks.signer import compute_signature

if TYPE_CHECKING:
    from aiokafka.structs import ConsumerRecord

    from eveys_ocpp.settings import Settings

log = get_logger(__name__)

# Backoff schedule per docs/integration/03-webhooks.md § Delivery
# semantics. Indexed by attempt number (0 = first attempt; we sleep
# this many seconds BEFORE the (n+1)th attempt).
_BACKOFF_SECONDS: tuple[float, ...] = (1.0, 5.0, 30.0, 120.0, 600.0)


class WebhookDispatcher:
    """Tail event topics, sign, deliver. One instance per process."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._http: httpx.AsyncClient | None = None
        self._shutdown = asyncio.Event()

    async def start(self) -> None:
        """Open the Kafka consumer and the HTTP client. Idempotent
        only in the sense that calling it twice is a programming
        error; tests reuse the same instance with the same start →
        run → stop lifecycle as the ingestor."""
        topics = self._enabled_topics()
        if not topics:
            log.info("webhook_dispatcher.no_enabled_events")
            return

        consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._settings.kafka_brokers,
            group_id=self._settings.webhook_consumer_group,
            client_id="eveys-ocpp-webhook-dispatcher",
            # Manual commit: only commit after a delivery attempt
            # ends (success OR exhausted-retries). At-least-once.
            enable_auto_commit=False,
            # Earliest so a brand-new dispatcher catches existing
            # rows on first deploy. Operators replay by deleting
            # and recreating the consumer group.
            auto_offset_reset="earliest",
        )
        await consumer.start()
        self._consumer = consumer

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
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None
        if self._http is not None:
            await self._http.aclose()
            self._http = None
        log.info("webhook_dispatcher.stopped")

    async def serve_forever(self) -> None:
        """Main loop: poll Kafka, deliver each record, commit offsets.

        Exits on `stop()`. Failures during a single delivery are
        contained — they don't tear the loop down. Failures during
        Kafka consumer ops (broker dropping, etc.) propagate so the
        TaskGroup can decide how to handle them."""
        if self._consumer is None or self._http is None:
            # `start()` returned early because no events are enabled.
            # Stay alive in case the TaskGroup is waiting on us, but do
            # nothing.
            await self._shutdown.wait()
            return

        consumer = self._consumer
        try:
            while not self._shutdown.is_set():
                # 1 s poll keeps shutdown latency low without burning CPU.
                batch = await consumer.getmany(timeout_ms=1000)
                if not batch:
                    continue

                for _tp, records in batch.items():
                    for record in records:
                        try:
                            await self._deliver_one(record)
                        except Exception as exc:
                            # Log + continue; never crash the loop on
                            # one bad record.
                            log.exception(
                                "webhook_dispatcher.unexpected_error",
                                topic=record.topic,
                                offset=record.offset,
                                error=str(exc),
                            )
                # Commit after each batch — at-least-once, same as the
                # ClickHouse ingestor pattern.
                await consumer.commit()
                # Sample the per-partition lag right after committing.
                # Cheap (one Kafka API call per assigned partition) and
                # only fires when there was traffic this iteration —
                # idle partitions don't poll the broker for nothing.
                await self._sample_consumer_lag(consumer)
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
        with retry. Returns when the delivery succeeds OR retries
        are exhausted; either way the offset is committed by the
        caller."""
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
            return

        body = self._build_body(envelope)
        if body is None:
            # Event type not enabled or unknown payload — silently skip.
            return

        url = self._url_for(envelope)
        if url is None:
            return  # disabled event type

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
        """POST with exponential backoff. Per spec, 4xx (except 429)
        is non-retryable — backend's bug, not transient."""
        assert self._http is not None  # narrowed by start()

        max_attempts = self._settings.webhook_max_attempts
        for attempt in range(1, max_attempts + 1):
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
                response = await self._http.post(url, content=body_bytes, headers=headers)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
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
                await asyncio.sleep(_BACKOFF_SECONDS[attempt - 1])
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
                return

            if response.status_code == 429 or response.status_code >= 500:
                log.warning(
                    "webhook.delivery_attempt_failed",
                    url=url,
                    event_id=event_id,
                    event_type=event_type,
                    attempt=attempt,
                    status=response.status_code,
                )
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(_BACKOFF_SECONDS[attempt - 1])
                continue

            # 4xx (other than 429) — backend rejected. Don't retry.
            log.error(
                "webhook.delivery_rejected",
                url=url,
                event_id=event_id,
                event_type=event_type,
                attempt=attempt,
                status=response.status_code,
                body=response.text[:200],
            )
            metrics_registry.WEBHOOK_DELIVERIES_TOTAL.labels(
                event_type=event_type, outcome="rejected"
            ).inc()
            return

        log.error(
            "webhook.delivery_failed",
            url=url,
            event_id=event_id,
            event_type=event_type,
            max_attempts=max_attempts,
        )
        metrics_registry.WEBHOOK_DELIVERIES_TOTAL.labels(
            event_type=event_type, outcome="failed"
        ).inc()

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
