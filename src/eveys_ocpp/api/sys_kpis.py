"""`GET /api/v1/sys/kpis` — single-roundtrip rollup for the Console
dashboard.

Returns the headline counts the index page renders as KPI tiles. One
endpoint instead of the four separate counts the dashboard would
otherwise have to fan out across — saves N HTTP round-trips on every
poll and keeps the page snappy at fleet scale.

Fields (all integers; null means "not yet implemented"):
- `online_count`         — number of chargers with a live WS (Redis).
- `total_count`          — number of chargers known to Postgres.
- `active_tx_count`      — open transactions right now.
- `tx_today_count`       — transactions started since UTC midnight.
- `faulted_count`        — chargers whose last_status is "Faulted".
- `energy_24h_wh`        — total Wh delivered in the trailing 24h.
                            null until backed by a ClickHouse rollup.

Auth follows the same bearer-token middleware as every other
`/api/v1/*` route. The Console exposes it as `GET /sys/kpis`,
proxied to this path.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Request

from eveys_ocpp.observability import get_logger
from eveys_ocpp.persistence.db import session_scope
from eveys_ocpp.persistence.repositories import count_charge_points, count_transactions

log = get_logger(__name__)

router = APIRouter(tags=["sys-kpis"])


@router.get(
    "/sys/kpis",
    summary="Headline counts for the Console dashboard",
)
async def get_sys_kpis(request: Request) -> dict[str, Any]:
    registry = request.app.state.registry

    # Redis online count. Falls back to None when there's no registry
    # wired (tests / dev-without-Redis) so the UI knows to render `—`
    # rather than a misleading zero.
    online_count: int | None
    if registry is None:
        online_count = None
    else:
        try:
            online_count = await registry.count_online()
        except Exception:  # Redis blip must not 500 the dashboard.
            online_count = None

    now = datetime.now(UTC)
    midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)

    async with session_scope(request.app.state.session_factory) as session:
        total_count = await count_charge_points(session)
        active_tx_count = await count_transactions(session, active=True)
        tx_today_count = await count_transactions(session, started_from=midnight)
        faulted_count = await count_charge_points(session, last_status="Faulted")

    # Energy delivered (24h) is a ClickHouse rollup; not implemented
    # yet. Surface as null so the UI can render an em-dash without
    # mistaking "no data" for "zero energy".
    energy_24h_wh: int | None = None
    _ = now - timedelta(hours=24)  # placeholder anchor for the future rollup

    return {
        "online_count": online_count,
        "total_count": total_count,
        "active_tx_count": active_tx_count,
        "tx_today_count": tx_today_count,
        "faulted_count": faulted_count,
        "energy_24h_wh": energy_24h_wh,
        "request_id": request.state.request_id,
    }
