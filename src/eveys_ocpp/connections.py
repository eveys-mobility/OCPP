"""In-process map of `cp_id` → live `EveysChargePoint`.

The Redis registry (E2-9) tells us *which pod* holds a charger's WS;
this module tells us *which Python object* holds it inside this pod.
gRPC handlers consult this map to find the connection object they
need to call OCPP requests on.

Both mutations are O(1) and synchronous (single-threaded asyncio
process — no lock needed). Lookups are O(1) too.

If a charger reconnects (network blip), the new connection inserts
itself; the disconnected old connection's `finally` block sees it's
no longer the registered owner and skips removal — same compare-
and-delete pattern as the Redis registry.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.connection import EveysChargePoint

log = get_logger(__name__)


class ConnectionMap:
    """Per-process registry of active charger WebSockets."""

    def __init__(self) -> None:
        self._map: dict[str, EveysChargePoint] = {}

    def add(self, cp: EveysChargePoint) -> None:
        """Register `cp` as the active connection for its `cp_id`.

        If a connection for the same `cp_id` already exists, it is
        replaced (the old one's WS is presumably half-dead or about
        to close).
        """
        existing = self._map.get(cp.id)
        self._map[cp.id] = cp
        if existing is not None and existing is not cp:
            log.warning("connections.replaced", cp_id=cp.id)

    def remove(self, cp: EveysChargePoint) -> bool:
        """Remove `cp` iff it's still the registered owner.

        Returns True if removed. False means a newer connection took
        ownership in between (reconnect race) — leave it alone.
        """
        current = self._map.get(cp.id)
        if current is cp:
            del self._map[cp.id]
            return True
        return False

    def get(self, cp_id: str) -> EveysChargePoint | None:
        """Return the active connection for `cp_id`, or None if not on this pod."""
        return self._map.get(cp_id)

    def __len__(self) -> int:
        return len(self._map)

    def __contains__(self, cp_id: str) -> bool:
        return cp_id in self._map
