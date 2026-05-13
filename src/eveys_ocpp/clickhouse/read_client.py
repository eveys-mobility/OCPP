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
        #
        # `asynch` substitutes via `query.format(**escape_params(params))`,
        # so placeholders are `{name}` — NOT the DB-API `%(name)s` shape.
        # See `fetch_latest_connector_statuses` for the same convention.
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
            WHERE cp_id = {cp_id}
              AND occurred_at >= {from_ts}
              AND occurred_at <= {to_ts}
        """
        params: dict[str, Any] = {
            "cp_id": cp_id,
            "from_ts": started_from,
            "to_ts": started_to,
        }
        if connector_id is not None:
            sql += " AND connector_id = {connector_id}"
            params["connector_id"] = connector_id
        if measurand is not None:
            sql += " AND measurand = {measurand}"
            params["measurand"] = measurand
        sql += " ORDER BY occurred_at, event_id LIMIT {limit}"
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

        # `asynch` substitutes parameters via `str.format(**escape_params(params))`,
        # so placeholders are `{name}` not the DB-API `%(name)s` paramstyle. The
        # IN list expands as one named placeholder per id — `escape_params`
        # quotes each value, so user-controlled cp_ids cannot break out of
        # the IN expression.
        placeholders = ", ".join(f"{{cp_id_{i}}}" for i in range(len(cp_ids)))
        # Fully-qualified table name — ClickHouse 24's analyzer does not
        # always honour the connection's `currentDatabase()` for unqualified
        # references in subqueries; spelling the database keeps both old
        # and new analyzers happy.
        db = self._settings.clickhouse_db
        sql = f"""
            SELECT
                cp_id,
                connector_id,
                argMax(status, occurred_at) AS status,
                argMax(error_code, occurred_at) AS error_code,
                max(occurred_at) AS last_changed_at
            FROM {db}.cp_status
            WHERE cp_id IN ({placeholders})
            GROUP BY cp_id, connector_id
            ORDER BY cp_id, connector_id
        """
        params: dict[str, Any] = {f"cp_id_{i}": cp_id for i, cp_id in enumerate(cp_ids)}

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

    async def fetch_transaction_telemetry(
        self,
        *,
        cp_id: str,
        transaction_id: int,
    ) -> dict[str, Any]:
        """Bounded telemetry snapshot for one transaction.

        Two scans of `cp_meter` filtered by (`cp_id`, `transaction_id`) —
        one for SoC start/last, one for per-phase voltage/current/power.
        The transaction id pins the work so a long-running session can't
        blow up the result set: at most 1 row from the SoC query and 9
        from the phases query (3 phases x 3 measurands).

        Returns a dict shaped for `TransactionTelemetry`:
            {
                "soc": {"start_pct": ..., "last_pct": ..., "last_at": ...},
                "phases": {"L1": {voltage_v, current_a, power_w, last_at}, ...},
            }

        Phases the charger never reported are absent from `phases`. SoC
        fields are `None` when no SoC sample exists for the transaction.
        """
        assert self._conn is not None  # narrowed by start()

        # Both queries restrict to (cp_id, transaction_id) so the
        # MergeTree partition + order key (cp_id, occurred_at) prunes
        # the scan. The string-cast on `value` matches `cp_meter`'s
        # column type — sampled values are stored as Strings (DDL
        # at clickhouse/ddl/0002_create_cp_meter.sql); cast at read.
        #
        # Storage form is the proto enum *name* (e.g. `MEASURAND_VOLTAGE`,
        # `PHASE_L1`) — written by the ingestor from `events_pb2.*.Name(v)`
        # against the values produced by `_ocpp_enums.MEASURAND_BY_OCPP`
        # in the MeterValues handler. Filter strings here MUST match that
        # form, not the OCPP wire form (`"Voltage"`, `"L1"`); the route
        # layer translates user-facing wire-form filter params on input.
        soc_sql = """
            SELECT
                argMin(toFloat64OrNull(sv.value), occurred_at) AS start_pct,
                argMax(toFloat64OrNull(sv.value), occurred_at) AS last_pct,
                max(occurred_at) AS last_at
            FROM cp_meter
            ARRAY JOIN sampled_values AS sv
            WHERE cp_id = {cp_id}
              AND transaction_id = {transaction_id}
              AND sv.measurand = 'MEASURAND_SOC'
        """
        phases_sql = """
            SELECT
                sv.phase AS phase,
                sv.measurand AS measurand,
                argMax(toFloat64OrNull(sv.value), occurred_at) AS value,
                max(occurred_at) AS last_at
            FROM cp_meter
            ARRAY JOIN sampled_values AS sv
            WHERE cp_id = {cp_id}
              AND transaction_id = {transaction_id}
              AND sv.phase IN ('PHASE_L1', 'PHASE_L2', 'PHASE_L3')
              AND sv.measurand IN (
                  'MEASURAND_VOLTAGE',
                  'MEASURAND_CURRENT_IMPORT',
                  'MEASURAND_POWER_ACTIVE_IMPORT'
              )
            GROUP BY phase, measurand
        """
        params: dict[str, Any] = {"cp_id": cp_id, "transaction_id": transaction_id}

        async with self._conn.cursor() as cursor:
            await cursor.execute(soc_sql, params)
            soc_cols = [d[0] for d in cursor.description]
            soc_rows = await cursor.fetchall()

            await cursor.execute(phases_sql, params)
            phase_cols = [d[0] for d in cursor.description]
            phase_rows = await cursor.fetchall()

        soc: dict[str, Any] = {"start_pct": None, "last_pct": None, "last_at": None}
        if soc_rows:
            row = dict(zip(soc_cols, soc_rows[0], strict=True))
            # `max(occurred_at)` over an empty filter yields the epoch sentinel
            # (1970-01-01) on ClickHouse — guard so we don't surface that as
            # a real timestamp.
            last_at = row["last_at"]
            has_data = row["start_pct"] is not None or row["last_pct"] is not None
            soc = {
                "start_pct": row["start_pct"],
                "last_pct": row["last_pct"],
                "last_at": last_at.isoformat() if has_data and last_at is not None else None,
            }

        # Keyed on the proto enum *name* form (storage form). The
        # response dict is keyed by the OCPP wire-form phase name
        # (`"L1"`, not `"PHASE_L1"`) so API consumers see the spec
        # form they already know.
        phases: dict[str, dict[str, Any]] = {}
        _measurand_to_field = {
            "MEASURAND_VOLTAGE": "voltage_v",
            "MEASURAND_CURRENT_IMPORT": "current_a",
            "MEASURAND_POWER_ACTIVE_IMPORT": "power_w",
        }
        _phase_to_wire = {
            "PHASE_L1": "L1",
            "PHASE_L2": "L2",
            "PHASE_L3": "L3",
        }
        for raw in phase_rows:
            row = dict(zip(phase_cols, raw, strict=True))
            phase = _phase_to_wire.get(row["phase"])
            field = _measurand_to_field.get(row["measurand"])
            if phase is None or field is None:
                continue
            snap = phases.setdefault(
                phase,
                {"voltage_v": None, "current_a": None, "power_w": None, "last_at": None},
            )
            snap[field] = row["value"]
            # `last_at` per phase = max occurred_at across that phase's
            # measurands (groupBy is per measurand; we fold here).
            row_last = row["last_at"]
            if row_last is not None and (snap["last_at"] is None or row_last > snap["last_at"]):
                snap["last_at"] = row_last

        # Format last_at strings only after the fold so comparisons
        # above stay on native datetimes.
        for snap in phases.values():
            if snap["last_at"] is not None:
                snap["last_at"] = snap["last_at"].isoformat()

        return {"soc": soc, "phases": phases}

    async def fetch_offline_history(
        self,
        *,
        cp_id: str,
        since: datetime | None,
        until: datetime | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        """Return offline-duration rows for one charger, plus the total
        row count matching the same filters.

        The bare-minimum filter surface — path-scoped `cp_id` plus the
        optional `[since, until]` window on `came_online_at` (the row's
        anchor timestamp, partition-aligned). No other filters per the
        feature scope.

        Returns `(rows, total)`. `rows` honours `limit` + `offset`;
        `total` is the unbounded count for offset-mode pagination.
        Both are computed in one round-trip via two cursor executes.
        """
        assert self._conn is not None  # narrowed by start()

        where = "WHERE cp_id = {cp_id}"
        params: dict[str, Any] = {"cp_id": cp_id}
        if since is not None:
            where += " AND came_online_at >= {since}"
            params["since"] = since
        if until is not None:
            where += " AND came_online_at <= {until}"
            params["until"] = until

        select_sql = f"""
            SELECT
                event_id,
                occurred_at,
                cp_id,
                went_offline_at,
                came_online_at,
                offline_seconds,
                prior_pod_id,
                prior_reason
            FROM cp_offline_duration
            {where}
            ORDER BY came_online_at DESC, event_id DESC
            LIMIT {{limit}} OFFSET {{offset}}
        """
        count_sql = f"""
            SELECT count() FROM cp_offline_duration
            {where}
        """
        select_params = {**params, "limit": limit, "offset": offset}

        async with self._conn.cursor() as cursor:
            await cursor.execute(select_sql, select_params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
            await cursor.execute(count_sql, params)
            count_rows = await cursor.fetchall()
        total = int(count_rows[0][0]) if count_rows else 0
        return [dict(zip(cols, row, strict=True)) for row in rows], total

    async def fetch_latest_meter_per_connector(
        self,
        *,
        cp_id: str,
    ) -> dict[int, dict[str, Any]]:
        """Return the latest `Energy.Active.Import.Register` sample per
        connector for one charger.

        Used to inline a live-meter readout on `/charge-points/{cp_id}`:
        the detail page wants "what does the meter say right now" without
        a second request to `/meter-values`. Returns a mapping keyed by
        `connector_id`; each value is `{value_wh, occurred_at}`.

        Storage form is the proto enum name (`MEASURAND_ENERGY_ACTIVE_
        IMPORT_REGISTER`) — same convention as `fetch_transaction_
        telemetry`. Bounded scan via the cp_id partition key.
        """
        assert self._conn is not None  # narrowed by start()

        sql = """
            SELECT
                connector_id,
                argMax(toFloat64OrNull(sv.value), occurred_at) AS value_wh,
                max(occurred_at) AS occurred_at
            FROM cp_meter
            ARRAY JOIN sampled_values AS sv
            WHERE cp_id = {cp_id}
              AND sv.measurand = 'MEASURAND_ENERGY_ACTIVE_IMPORT_REGISTER'
            GROUP BY connector_id
        """
        params: dict[str, Any] = {"cp_id": cp_id}

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()

        out: dict[int, dict[str, Any]] = {}
        for row in rows:
            record = dict(zip(cols, row, strict=True))
            connector_id = int(record["connector_id"])
            # argMax over zero rows would surface as None; the GROUP BY
            # already excludes that case, but guard for safety.
            if record["value_wh"] is None:
                continue
            out[connector_id] = {
                "value_wh": record["value_wh"],
                "occurred_at": record["occurred_at"],
            }
        return out

    async def fetch_latest_offline_durations(
        self,
        *,
        cp_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """Return the most recent offline-duration row per cp_id.

        Used by `/charge-points` list and detail to inline a
        `last_offline_seconds` + `last_offline_ended_at` summary so a
        Console operator can see the last outage without a second
        round-trip. Chargers that have never had an outage observed by
        the gateway are absent from the result.

        Same shape and IN-list pattern as `fetch_latest_connector_
        statuses` — one ClickHouse scan with `argMax` keyed on
        `came_online_at`, partition prune via the cp_id IN list.
        """
        assert self._conn is not None  # narrowed by start()

        if not cp_ids:
            return {}

        placeholders = ", ".join(f"{{cp_id_{i}}}" for i in range(len(cp_ids)))
        db = self._settings.clickhouse_db
        sql = f"""
            SELECT
                cp_id,
                argMax(offline_seconds, came_online_at) AS offline_seconds,
                max(came_online_at) AS last_offline_ended_at
            FROM {db}.cp_offline_duration
            WHERE cp_id IN ({placeholders})
            GROUP BY cp_id
        """
        params: dict[str, Any] = {f"cp_id_{i}": cp_id for i, cp_id in enumerate(cp_ids)}

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()

        out: dict[str, dict[str, Any]] = {}
        for row in rows:
            record = dict(zip(cols, row, strict=True))
            cp_id = record.pop("cp_id")
            out[cp_id] = record
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

        # Same `{name}` placeholder convention as `fetch_meter_values` /
        # `fetch_latest_connector_statuses` — `asynch` uses
        # `query.format(**escape_params(params))`, not DB-API
        # `%(name)s`.
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
            WHERE cp_id = {cp_id}
              AND occurred_at >= {from_ts}
              AND occurred_at <= {to_ts}
        """
        params: dict[str, Any] = {
            "cp_id": cp_id,
            "from_ts": started_from,
            "to_ts": started_to,
        }
        if connector_id is not None:
            sql += " AND connector_id = {connector_id}"
            params["connector_id"] = connector_id
        sql += " ORDER BY occurred_at, event_id LIMIT {limit}"
        params["limit"] = limit

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
        return [dict(zip(cols, row, strict=True)) for row in rows]

    async def fetch_fleet_status_history(
        self,
        *,
        started_from: datetime,
        started_to: datetime,
        statuses: list[str] | None,
        cp_ids: list[str] | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Return StatusNotification transitions across the fleet inside
        [from, to], optionally filtered by status value and/or a small
        list of cp_ids.

        Fleet-wide variant of :meth:`fetch_status_history`. Same row
        shape and ordering; just drops the single-cp filter. The
        partition prune on ``occurred_at`` keeps the scan bounded to
        the months that overlap the window.

        Common use: "all Faulted statuses this week" — pass
        ``statuses=["Faulted"]`` and a 7-day window.
        """
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
            WHERE occurred_at >= {from_ts}
              AND occurred_at <= {to_ts}
        """
        params: dict[str, Any] = {
            "from_ts": started_from,
            "to_ts": started_to,
        }
        if statuses:
            # asynch's str.format escaping doesn't handle list literals
            # natively, so render the IN clause's element placeholders
            # by name and bind each value individually.
            placeholders = ", ".join(f"{{status_{i}}}" for i in range(len(statuses)))
            sql += f" AND status IN ({placeholders})"
            for i, s in enumerate(statuses):
                params[f"status_{i}"] = s
        if cp_ids:
            placeholders = ", ".join(f"{{cp_{i}}}" for i in range(len(cp_ids)))
            sql += f" AND cp_id IN ({placeholders})"
            for i, c in enumerate(cp_ids):
                params[f"cp_{i}"] = c
        sql += " ORDER BY occurred_at, event_id LIMIT {limit}"
        params["limit"] = limit

        async with self._conn.cursor() as cursor:
            await cursor.execute(sql, params)
            cols = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
        return [dict(zip(cols, row, strict=True)) for row in rows]
