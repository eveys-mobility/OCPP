"""ChargePoint subclass.

Wires `mobilityhouse/ocpp`'s `ChargePoint` to our handlers + persistence.
Handlers live in `handlers/v16/` and are registered on the instance via
`@on()` decorators (the library's own routing).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

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
    meter_values,
    start_transaction,
    status_notification,
    stop_transaction,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from websockets.asyncio.server import ServerConnection

    from eveys_ocpp.events import EventProducer
    from eveys_ocpp.idempotency import IdempotencyCache
    from eveys_ocpp.platform import AuthorizeCache, BackendHTTPClient
    from eveys_ocpp.registry import Registry
    from eveys_ocpp.settings import Settings


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
    ) -> None:
        super().__init__(cp_id, connection)
        self.session_factory = session_factory
        self.settings = settings
        self.registry = registry
        self.event_producer = event_producer
        self.idempotency = idempotency
        self.backend_client = backend_client
        self.authorize_cache = authorize_cache

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
