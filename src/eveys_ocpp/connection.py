"""ChargePoint subclass.

Wires `mobilityhouse/ocpp`'s `ChargePoint` to our handlers + persistence.
Handlers live in `handlers/v16/` and are registered on the instance via
`@on()` decorators (the library's own routing).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ocpp.messages import MessageType, unpack
from ocpp.routing import on
from ocpp.v16 import ChargePoint as Cpv16
from ocpp.v16 import call_result
from ocpp.v16.enums import Action

from eveys_ocpp.handlers.v16 import (
    authorize,
    boot_notification,
    data_transfer,
    diagnostics_status_notification,
    firmware_status_notification,
    heartbeat,
    log_status_notification,
    meter_values,
    security_event_notification,
    start_transaction,
    status_notification,
    stop_transaction,
)
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import get_logger

log = get_logger(__name__)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from websockets.asyncio.server import ServerConnection

    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.idempotency import IdempotencyCache
    from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings
    from eveys_ocpp.transport._rate_limiter import RateLimiter


class EveysChargePoint(Cpv16):
    """One instance per connected charger.

    Holds:
    - the WebSocket
    - process-wide session factory + settings
    - Redis registry handle (None in unit tests + Kafka-less stacks)
    - Kafka event producer (None in unit tests + Kafka-less stacks)
    - Backend HTTP client (None in unit tests / when backend_base_url is empty)
    - Authorize cache (None when Redis is unavailable or `backend_authorize_cache_enabled=False`)
    - per-charger logging context
    """

    def __init__(
        self,
        cp_id: str,
        connection: ServerConnection,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
        registry: Registry | None = None,
        event_producer: EventProducer | None = None,
        idempotency: IdempotencyCache | None = None,
        backend_client: BackendHTTPClient | None = None,
        authorize_cache: AuthorizeCache | None = None,
        rate_limiter: RateLimiter | None = None,
    ) -> None:
        super().__init__(cp_id, connection)
        self.session_factory = session_factory
        self.settings = settings
        self.registry = registry
        self.event_producer = event_producer
        self.idempotency = idempotency
        self.backend_client = backend_client
        self.authorize_cache = authorize_cache
        self.rate_limiter = rate_limiter

    # ---- handler delegation -------------------------------------------------
    # Each handler module exports a `handle(...)` coroutine. We thin-wrap
    # them here so the decorator metadata sits on this class (where the
    # ocpp library inspects it) but the bodies live in their own files.
    #
    # `**kwargs: Any` is intentional here: the ocpp library passes a kwargs
    # dict whose key/value shapes are validated against the OCPP JSON Schema
    # before reaching us, so the handler's keyword-only parameters carry the
    # real types. Tightening this to a Protocol/TypedDict would mean
    # restating the schema — net negative.

    # Two handlers must be idempotent on inbound replays (AGENTS rule 3,
    # E2-11). We add `call_unique_id` to the dispatch signature so the
    # mobilityhouse/ocpp library passes the OCPP frame's MessageId — that's
    # the dedup key. The library only forwards the kwarg if it's named
    # explicitly in the signature; with bare `**kwargs` it's omitted.

    @on(Action.boot_notification)
    async def on_boot_notification(
        self, *, call_unique_id: str | None = None, **kwargs: Any
    ) -> call_result.BootNotification:
        return await boot_notification.handle(self, message_id=call_unique_id, **kwargs)

    @on(Action.heartbeat)
    async def on_heartbeat(self) -> call_result.Heartbeat:
        return await heartbeat.handle(self)

    @on(Action.status_notification)
    async def on_status_notification(self, **kwargs: Any) -> call_result.StatusNotification:
        return await status_notification.handle(self, **kwargs)

    @on(Action.authorize)
    async def on_authorize(
        self, *, call_unique_id: str | None = None, **kwargs: Any
    ) -> call_result.Authorize:
        return await authorize.handle(self, message_id=call_unique_id, **kwargs)

    @on(Action.start_transaction)
    async def on_start_transaction(self, **kwargs: Any) -> call_result.StartTransaction:
        return await start_transaction.handle(self, **kwargs)

    @on(Action.stop_transaction)
    async def on_stop_transaction(
        self, *, call_unique_id: str | None = None, **kwargs: Any
    ) -> call_result.StopTransaction:
        return await stop_transaction.handle(self, message_id=call_unique_id, **kwargs)

    @on(Action.meter_values)
    async def on_meter_values(self, **kwargs: Any) -> call_result.MeterValues:
        return await meter_values.handle(self, **kwargs)

    @on(Action.data_transfer)
    async def on_data_transfer(self, **kwargs: Any) -> call_result.DataTransfer:
        return await data_transfer.handle(self, **kwargs)

    @on(Action.diagnostics_status_notification)
    async def on_diagnostics_status_notification(
        self, **kwargs: Any
    ) -> call_result.DiagnosticsStatusNotification:
        return await diagnostics_status_notification.handle(self, **kwargs)

    @on(Action.firmware_status_notification)
    async def on_firmware_status_notification(
        self, **kwargs: Any
    ) -> call_result.FirmwareStatusNotification:
        return await firmware_status_notification.handle(self, **kwargs)

    @on(Action.security_event_notification)
    async def on_security_event_notification(
        self, **kwargs: Any
    ) -> call_result.SecurityEventNotification:
        return await security_event_notification.handle(self, **kwargs)

    @on(Action.log_status_notification)
    async def on_log_status_notification(self, **kwargs: Any) -> call_result.LogStatusNotification:
        return await log_status_notification.handle(self, **kwargs)

    # ---- Prometheus instrumentation hooks (E4-1) ----------------------------
    # Override the library's inbound and outbound dispatch points so we
    # can count every OCPP CALL crossing this connection without
    # touching each handler. The library handles the actual routing /
    # response queueing — we just observe.

    async def route_message(self, raw_msg: str) -> None:
        """Tap the inbound dispatch path for the WS_MESSAGES_IN counter
        and the E5-3 per-charger rate limiter.

        We unpack twice (once here for the action label, once again
        inside `super().route_message`); a second `unpack` call on the
        same string is cheap and avoids reimplementing the library's
        Call/CallResult/CallError branching.

        Malformed frames raise during the first unpack — we count them
        under action="_invalid" so the metric stays bounded and the
        library's own error path runs unchanged.

        Rate limiting (E5-3): only **CALLs** are throttled. CALLRESULT /
        CALLERROR are correlated responses to commands we sent;
        throttling them would break our own RemoteStart / Reset / etc.
        flows. On overrun the message is dropped silently — see
        `_rate_limiter.py` for the full why.
        """
        action = "_invalid"
        is_call = False
        try:
            msg = unpack(raw_msg)
            is_call = msg.message_type_id == MessageType.Call
            # CALLRESULT / CALLERROR don't carry an action; bucket
            # them under "_response" to keep cardinality bounded.
            action = msg.action if is_call else "_response"
        except Exception:
            # Library logs + ignores malformed frames; we mirror by
            # tagging _invalid so the count stays visible.
            pass
        metrics_registry.WS_MESSAGES_IN_TOTAL.labels(action=action).inc()

        # Hot-checked via the runtime-override layer so an admin can
        # flip the rate limiter without a pod restart. Default is
        # the boot-time setting; the override takes precedence when
        # set via PATCH /api/v1/admin/config.
        from eveys_ocpp.runtime_overrides import get_override

        rate_limit_enabled = get_override(
            "ws_rate_limit_enabled", self.settings.ws_rate_limit_enabled
        )
        if is_call and self.rate_limiter is not None and rate_limit_enabled:
            allowed = await self.rate_limiter.check(self.id)
            if not allowed:
                await self.rate_limiter.record_throttled(action=action)
                # Log carries cp_id + action so an operator can find
                # the offender; metric label stays bounded.
                log.warning("rate_limit.throttled", cp_id=self.id, action=action)
                return  # drop the frame; library never sees it

        await super().route_message(raw_msg)

    async def call(
        self,
        payload: Any,
        suppress: bool = True,
        unique_id: str | None = None,
        skip_schema_validation: bool = False,
    ) -> Any:
        """Tap the outbound dispatch path for the WS_MESSAGES_OUT counter.

        The action name is the payload's class name, mirroring the
        library's own derivation in `ChargePoint.call`.
        """
        action = type(payload).__name__
        metrics_registry.WS_MESSAGES_OUT_TOTAL.labels(action=action).inc()
        return await super().call(
            payload,
            suppress=suppress,
            unique_id=unique_id,
            skip_schema_validation=skip_schema_validation,
        )
