"""Async ClickHouse read client for the gateway REST timeseries endpoints (E3-7d).

The ingestor (`ingestor.py`) writes; this module reads. Single
long-lived `Connection` per process opened at app boot; each query
opens a transient cursor. Mirrors the host/port/db settings the writer
already uses.

Two query helpers — meter values and status history — both bounded by
a `[from, to]` window on the trustworthy `occurred_at` column. The
route layer enforces the 7-day window cap and turns excess into
`WINDOW_TOO_LARGE` (400). The read client itself trusts its callers.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from asynch import Connection

from eveys_ocpp.observability import get_logger

if TYPE_CHECKING:
    from eveys_ocpp.settings import Settings

log = get_logger(__name__)


class ClickHouseReadClient:
    """Lightweight wrapper around a single `asynch.Connection`.

    Owns the connection lifecycle. Use one instance per process,
    `start()` at boot, `aclose()` on shutdown. Each query opens a
    cursor; cursors are not reused across queries (asynch's cursor
    state is per-query)."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._conn: Connection | None = None

    async def start(self) -> None:
        conn = Connection(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            database=self._settings.clickhouse_db,
        )
        await conn.connect()
        self._conn = conn
        log.info(
            "clickhouse.read_client.started",
            clickhouse_host=self._settings.clickhouse_host,
            clickhouse_db=self._settings.clickhouse_db,
        )

    async def aclose(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
            log.info("clickhouse.read_client.stopped")

    async def fetch_meter_values(
        self,
        *,
        cp_id: str,
        started_from: datetime,
        started_to: datetime,
        connector_id: int | None,
        measurand: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return MeterValues rows for one charger inside [from, to].

        Rows are ARRAY-JOINed so each sampled value is its own row —
        the contract surfaces every sample independently rather than
        forcing the client to walk parallel arrays.
        """
        assert self._conn is not None  # narrowed by start()

        # ARRAY JOIN flattens the Nested column; the route caller pages
        # with `limit + 1` for next-page detection.
        sql = """
            SELECT
                event_id,
                occurred_at,
                cp_id,
                connector_id,
                transaction_id,
                charger_reported_at,
                sv.value AS value,
                sv.context AS context,
                sv.format AS format,
                sv.measurand AS measurand,
                sv.phase AS phase,
                sv.location AS location,
                sv.unit AS unit
            FROM cp_meter
            ARRAY JOIN sampled_values AS sv
            WHERE cp_id = %(cp_id)s
              AND occurred_at >= %(from_ts)s
              AND occurred_at <= %(to_ts)s
        """
        params: dict[str, Any] = {
            "cp_id": cp_id,
            "from_ts": started_from,
            "to_ts": started_to,
        }
        if connector_id is not None:
            sql += " AND connector_id = %(connector_id)s"
            params["connector_id"] = connector_id
        if measurand is not None:
            sql += " AND measurand = %(measurand)s"
            params["measurand"] = measurand
        sql += " ORDER BY occurred_at, event_id LIMIT %(limit)s"
        params["limit"] = limit

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
        return [dict(zip(cols, row, strict=True)) for row in rows]

    async def fetch_latest_connector_statuses(
        self,
        *,
        cp_ids: list[str],
    ) -> dict[str, list[dict[str, Any]]]:
        """Return the most recent StatusNotification per (cp_id, connector_id).

        Used to enrich the `/charge-points` and `/charge-points/{cp_id}`
        responses with a per-connector breakdown, so a multi-connector
        charger no longer collapses to a single `last_status` slot. The
        scalar `last_status` is kept on the response as a "most recent
        across all connectors" convenience for single-connector callers
        and consumers who don't need the breakdown.

        Returns a mapping keyed by cp_id; chargers with no recorded
        StatusNotifications are simply absent from the map. Callers
        treat that as "no per-connector data, fall back to []".

        Implementation note: ClickHouse's `argMax` makes "latest row per
        group" cheap — one scan, no subqueries — and the cp_status
        partition key starts with cp_id so the IN-list prunes well.
        """
        assert self._conn is not None  # narrowed by start()

        if not cp_ids:
            return {}

        sql = """
            SELECT
                cp_id,
                connector_id,
                argMax(status, occurred_at) AS status,
                argMax(error_code, occurred_at) AS error_code,
                max(occurred_at) AS last_changed_at
            FROM cp_status
            WHERE cp_id IN %(cp_ids)s
            GROUP BY cp_id, connector_id
            ORDER BY cp_id, connector_id
        """
        params: dict[str, Any] = {"cp_ids": tuple(cp_ids)}

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()

        out: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            record = dict(zip(cols, row, strict=True))
            cp_id = record.pop("cp_id")
            out.setdefault(cp_id, []).append(record)
        return out

    async def fetch_status_history(
        self,
        *,
        cp_id: str,
        started_from: datetime,
        started_to: datetime,
        connector_id: int | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return StatusNotification transitions for one charger inside
        [from, to]. One row per transition (no ARRAY JOIN needed)."""
        assert self._conn is not None  # narrowed by start()

        sql = """
            SELECT
                event_id,
                occurred_at,
                cp_id,
                connector_id,
                status,
                error_code,
                info,
                vendor_id,
                vendor_error_code,
                charger_reported_at
            FROM cp_status
            WHERE cp_id = %(cp_id)s
              AND occurred_at >= %(from_ts)s
              AND occurred_at <= %(to_ts)s
        """
        params: dict[str, Any] = {
            "cp_id": cp_id,
            "from_ts": started_from,
            "to_ts": started_to,
        }
        if connector_id is not None:
            sql += " AND connector_id = %(connector_id)s"
            params["connector_id"] = connector_id
        sql += " ORDER BY occurred_at, event_id LIMIT %(limit)s"
        params["limit"] = limit

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
        return [dict(zip(cols, row, strict=True)) for row in rows]
