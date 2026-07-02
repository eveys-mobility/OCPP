"""DataTransfer handler (charger-initiated direction).

OCPP 1.6 reference: DataTransfer.req / DataTransfer.conf, Core profile.
The protocol's vendor-extension envelope: chargers can emit arbitrary
``vendor_id``-namespaced payloads alongside the standard surface. The
CSMS-initiated direction is in ``transport/grpc_server.py::DataTransfer``.

Behaviour rule of thumb for the inbound side: we have no a priori
knowledge of which ``vendor_id`` strings the operator wants to honour,
so we acknowledge with ``UnknownVendorId`` by default and rely on
operators wiring vendor handlers explicitly when needed (a future
hook lives in `Settings.data_transfer_vendor_allowlist` — empty for
now). This is the spec-compliant negative path: the charger learns
"I don't know this vendor" and stops sending.

Why we don't crash or auto-accept: per AGENTS rule 3, replies must
be deterministic and validated. Auto-accepting an arbitrary
vendor-extension payload masks a misconfigured charger or a
mislabeled firmware. ``UnknownVendorId`` is the spec's way of saying
"please stop talking to me about this".
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from ocpp.exceptions import SecurityError
from ocpp.v16 import call_result
from ocpp.v16.enums import DataTransferStatus

from eveys_ocpp.metrics import record_handler_error, time_handler
from eveys_ocpp.metrics import registry as metrics_registry
from eveys_ocpp.observability import bind_contextvars, get_logger

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


async def handle(
    cp: EveysChargePoint,
    *,
    vendor_id: str,
    message_id: str | None = None,
    data: str | None = None,
) -> call_result.DataTransfer:
    bind_contextvars(cp_id=cp.id, action="DataTransfer", direction="rx")

    # Pending-authorization gate — only BootNotification is honoured
    # while the operator hasn't approved this cp_id. `SecurityError`
    # is untyped upstream (mobilityhouse/ocpp ships no `py.typed`).
    if cp.is_pending:
        raise SecurityError(  # type: ignore[no-untyped-call]
            details={"reason": "authorization pending; operator has not authorized this cp_id"}
        )

    with time_handler("DataTransfer"):
        try:
            log.info(
                "data_transfer.received",
                vendor_id=vendor_id,
                message_id=message_id,
                data_len=len(data) if data is not None else 0,
            )

            # No vendor handlers wired today — return UnknownVendorId per spec.
            # If/when operators want to honour a vendor, the dispatch table goes
            # here. Keeping the surface small means we don't accidentally
            # accept payloads we can't actually process.
            status = DataTransferStatus.unknown_vendor_id
            metrics_registry.DATA_TRANSFERS_TOTAL.labels(
                vendor_id=vendor_id, status=status.value
            ).inc()
            return call_result.DataTransfer(status=status)
        except Exception as exc:
            record_handler_error("DataTransfer", exc)
            raise
