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
    heartbeat,
    start_transaction,
    status_notification,
    stop_transaction,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from websockets.asyncio.server import ServerConnection

    from eveys_ocpp.settings import Settings


class EveysChargePoint(Cpv16):
    """One instance per connected charger.

    Holds:
    - the WebSocket
    - a reference to the process-wide session factory + settings
    - per-charger context for logging
    """

    def __init__(
        self,
        cp_id: str,
        connection: ServerConnection,
        *,
        session_factory: async_sessionmaker[AsyncSession],
        settings: Settings,
    ) -> None:
        super().__init__(cp_id, connection)
        self.session_factory = session_factory
        self.settings = settings

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

    @on(Action.boot_notification)
    async def on_boot_notification(self, **kwargs: Any) -> call_result.BootNotification:
        return await boot_notification.handle(self, **kwargs)

    @on(Action.heartbeat)
    async def on_heartbeat(self) -> call_result.Heartbeat:
        return await heartbeat.handle(self)

    @on(Action.status_notification)
    async def on_status_notification(self, **kwargs: Any) -> call_result.StatusNotification:
        return await status_notification.handle(self, **kwargs)

    @on(Action.authorize)
    async def on_authorize(self, **kwargs: Any) -> call_result.Authorize:
        return await authorize.handle(self, **kwargs)

    @on(Action.start_transaction)
    async def on_start_transaction(self, **kwargs: Any) -> call_result.StartTransaction:
        return await start_transaction.handle(self, **kwargs)

    @on(Action.stop_transaction)
    async def on_stop_transaction(self, **kwargs: Any) -> call_result.StopTransaction:
        return await stop_transaction.handle(self, **kwargs)
