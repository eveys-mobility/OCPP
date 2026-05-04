"""End-to-end test: ClickHouse migrations land the four event tables.

Runs `apply_pending` against the live ClickHouse in compose/CI, then
verifies all five expected tables exist with the right column shape.

Skipped when ClickHouse isn't reachable (dev laptop with no compose
stack); hard-fails on missing-but-required service when
``E2E_REQUIRE=1`` is set (CI). Same skip-vs-fail policy the rest of
the e2e suite uses.
"""

from __future__ import annotations

import os
import socket
import urllib.parse
import urllib.request
from contextlib import closing

import pytest

from eveys_ocpp.clickhouse.migrate import apply_pending

_CH_HOST = os.environ.get("E2E_CH_HOST", "localhost")
_CH_HTTP_PORT = int(os.environ.get("E2E_CH_HTTP_PORT", "8123"))
_CH_DB = os.environ.get("EVEYS_OCPP_CLICKHOUSE_DB", "eveys_ocpp")
_E2E_REQUIRE = os.environ.get("E2E_REQUIRE") == "1"


def _ch_reachable() -> bool:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.settimeout(0.5)
        try:
            s.connect((_CH_HOST, _CH_HTTP_PORT))
        except OSError:
            return False
        return True


if not _ch_reachable():
    _msg = f"ClickHouse at {_CH_HOST}:{_CH_HTTP_PORT} unreachable; schema test needs it"
    if _E2E_REQUIRE:
        pytest.fail(
            f"{_msg}. E2E_REQUIRE=1 — the tests:e2e job must keep its "
            "`clickhouse` service. CI config bug, not env issue.",
            pytrace=False,
        )
    pytestmark = pytest.mark.skip(reason=_msg)


def _query(sql: str) -> str:
    """Run a SELECT and return the response body."""
    url = f"http://{_CH_HOST}:{_CH_HTTP_PORT}/?{urllib.parse.urlencode({'database': _CH_DB})}"
    req = urllib.request.Request(url, data=sql.encode("utf-8"), method="POST")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.read().decode("utf-8")


def test_apply_pending_creates_all_tables() -> None:
    """A clean migration run lands every event-table with the
    right column shape.

    The migrator is idempotent: running it twice in a row is fine,
    and previous test runs may have already applied the migrations.
    Either way, after the call returns, the five tables exist and
    `schema_migrations` has all five rows.
    """
    apply_pending(host=_CH_HOST, port=_CH_HTTP_PORT, db=_CH_DB)

    body = _query(
        "SELECT name FROM system.tables "
        f"WHERE database = '{_CH_DB}' "
        "ORDER BY name "
        "FORMAT TabSeparated"
    )
    tables = {line.strip() for line in body.splitlines() if line.strip()}
    expected = {"schema_migrations", "cp_meter", "cp_status", "cp_boot", "tx_started"}
    assert expected <= tables, f"missing tables: {sorted(expected - tables)}; got: {sorted(tables)}"


def test_schema_migrations_records_every_applied_version() -> None:
    """`schema_migrations` carries one row per DDL file."""
    apply_pending(host=_CH_HOST, port=_CH_HTTP_PORT, db=_CH_DB)

    body = _query("SELECT version FROM schema_migrations ORDER BY version FORMAT TabSeparated")
    versions = [int(line) for line in body.splitlines() if line.strip()]
    # Five DDL files exist as of this MR (E2-13/E2-14): 0001..0005.
    # The check is `>=` so future migrations don't break this test.
    assert versions[:5] == [1, 2, 3, 4, 5]


def test_cp_meter_has_nested_sampled_values() -> None:
    """The `Nested` column shape on cp_meter is what the ingestor expects.

    Every parallel array under `sampled_values.*` exists, and the
    type pattern (Array(String)) matches what `_row_cp_meter` emits.
    """
    apply_pending(host=_CH_HOST, port=_CH_HTTP_PORT, db=_CH_DB)

    body = _query(
        "SELECT name, type FROM system.columns "
        f"WHERE database = '{_CH_DB}' AND table = 'cp_meter' "
        "ORDER BY name FORMAT TabSeparated"
    )
    cols = {line.split("\t")[0]: line.split("\t")[1] for line in body.splitlines() if line.strip()}

    expected_nested = {
        "sampled_values.value",
        "sampled_values.context",
        "sampled_values.format",
        "sampled_values.measurand",
        "sampled_values.phase",
        "sampled_values.location",
        "sampled_values.unit",
    }
    missing = expected_nested - cols.keys()
    assert not missing, f"cp_meter is missing nested columns: {sorted(missing)}"
    for name in expected_nested:
        assert "Array(String)" in cols[name], (
            f"cp_meter.{name} should be Array(String), got: {cols[name]}"
        )
