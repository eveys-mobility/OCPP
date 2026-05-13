"""Unit tests for the ClickHouse migrator (E2-13, ADR-0020).

The end-to-end "migrations actually land in ClickHouse" check lives
in `tests/e2e/test_clickhouse_schema.py`. These are the unit-level
checks for the migrator's pure logic — filename parsing, ordering,
the bootstrap UNKNOWN_TABLE path, the URL builder. They run without
any ClickHouse instance: `urllib.request.urlopen` is patched.

Required because the unit-test job (`tests` in `.gitlab-ci.yml`) only
brings up Redis. ClickHouse runs in `tests:e2e`. Without these unit
tests, `migrate.py` sits at 0% coverage in CI and drags the total
under the 80% gate — exactly the false-green pattern AGENTS.md
warns against.
"""

from __future__ import annotations

import io
import urllib.error
from unittest.mock import MagicMock

import pytest

from eveys_ocpp.clickhouse import migrate

# ---- _read_migrations: filename convention --------------------------------


def test_read_migrations_returns_versioned_tuples_sorted() -> None:
    """The five DDL files in src/eveys_ocpp/clickhouse/ddl/ parse out
    in version order with the (version, name, sql) shape migrate.py
    uses."""
    rows = migrate._read_migrations()

    assert len(rows) >= 5  # 5 today; future migrations are fine
    assert [r[0] for r in rows[:5]] == [1, 2, 3, 4, 5]
    assert [r[1] for r in rows[:5]] == [
        "create_schema_migrations",
        "create_cp_meter",
        "create_cp_status",
        "create_cp_boot",
        "create_tx_started",
    ]
    # Each row carries the SQL body — non-empty, contains a CREATE
    # TABLE statement (the migrator passes this verbatim to ClickHouse).
    for _v, _n, sql in rows[:5]:
        assert "CREATE TABLE" in sql


def test_read_migrations_ignores_non_matching_filenames(
    tmp_path: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Files that don't match `NNNN_<name>.sql` are silently skipped.
    Lets a `.gitkeep` or a `README.md` live in the DDL dir without
    breaking the loader."""
    from pathlib import Path

    fake_ddl = Path(str(tmp_path)) / "ddl"
    fake_ddl.mkdir()
    (fake_ddl / "0001_first.sql").write_text("CREATE TABLE first (v Int32) ENGINE = Memory")
    (fake_ddl / "README.md").write_text("not a migration")
    (fake_ddl / "scratch.sql").write_text("not a migration either")
    (fake_ddl / "0002_second.sql").write_text("CREATE TABLE second (v Int32) ENGINE = Memory")

    monkeypatch.setattr(migrate, "_DDL_DIR", fake_ddl)
    rows = migrate._read_migrations()
    assert [(r[0], r[1]) for r in rows] == [(1, "first"), (2, "second")]


# ---- _execute: URL builder ------------------------------------------------


class _FakeResponse:
    """Mimics the urllib.request.urlopen context-manager response."""

    def __init__(self, body: bytes) -> None:
        self._body = body

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


def test_execute_builds_url_with_database_query_param(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        captured["url"] = req.full_url  # type: ignore[attr-defined]
        captured["data"] = req.data  # type: ignore[attr-defined]
        return _FakeResponse(b"ok\n")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    body = migrate._execute("ch", 8123, "eveys_ocpp", "SELECT 1")

    assert body == "ok\n"
    assert captured["url"] == "http://ch:8123/?database=eveys_ocpp"
    assert captured["data"] == b"SELECT 1"


def test_execute_omits_database_param_when_db_is_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`db=None` runs the statement without a default database — the
    bootstrap path used by `_ensure_database`."""
    captured: dict[str, object] = {}

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        captured["url"] = req.full_url  # type: ignore[attr-defined]
        return _FakeResponse(b"")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    migrate._execute("ch", 8123, None, "CREATE DATABASE foo")

    # No `?database=` in the URL — important for the bootstrap call
    # which runs *before* the database exists.
    assert captured["url"] == "http://ch:8123/"


# ---- _applied_versions: bootstrap UNKNOWN_TABLE path ----------------------


def test_applied_versions_returns_empty_set_on_unknown_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First-run bootstrap: schema_migrations doesn't exist yet,
    ClickHouse returns UNKNOWN_TABLE (error code 60). The migrator
    must treat that as 'no migrations applied yet', NOT propagate
    the error."""

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(
            url="http://ch:8123/",
            code=404,
            msg="Not Found",
            hdrs=None,  # type: ignore[arg-type]
            fp=io.BytesIO(b"Code: 60. DB::Exception: UNKNOWN_TABLE"),
        )

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    versions = migrate._applied_versions(host="ch", port=8123, db="eveys_ocpp")
    assert versions == set()


def test_applied_versions_propagates_other_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A non-UNKNOWN_TABLE error is fatal — don't swallow it,
    otherwise migrations would silently re-run against a broken
    database."""

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        raise urllib.error.HTTPError(
            url="http://ch:8123/",
            code=500,
            msg="Internal Server Error",
            hdrs=None,  # type: ignore[arg-type]
            fp=io.BytesIO(b"Code: 999. DB::Exception: SOMETHING_ELSE"),
        )

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    with pytest.raises(urllib.error.HTTPError):
        migrate._applied_versions(host="ch", port=8123, db="eveys_ocpp")


def test_applied_versions_parses_tab_separated_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """schema_migrations.version returns one int per line in
    TabSeparated format. Verify the parser builds the right set."""

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        return _FakeResponse(b"1\n2\n5\n\n")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    versions = migrate._applied_versions(host="ch", port=8123, db="eveys_ocpp")
    assert versions == {1, 2, 5}


# ---- apply_pending: end-to-end with mocked transport ----------------------


def test_apply_pending_applies_only_unapplied_migrations(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Versions 1 + 2 already applied. apply_pending should:
    1. CREATE DATABASE IF NOT EXISTS (always)
    2. SELECT version FROM schema_migrations
    3. Skip 1 and 2; apply only 3, 4, 5
    4. INSERT a tracking row after each successfully-applied DDL.

    Asserts the exact SQL execution order."""

    sqls_executed: list[tuple[str | None, str]] = []
    response_body = b"1\n2\n"  # versions 1 and 2 already applied

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        # Capture (database, sql) for every call. Database comes from
        # the URL query string; sql is the request body.
        url = req.full_url  # type: ignore[attr-defined]
        data = req.data  # type: ignore[attr-defined]
        db = None
        if "?database=" in url:
            db = url.split("?database=", 1)[1]
        sqls_executed.append((db, data.decode()))
        # First call is CREATE DATABASE (no db); next is SELECT
        # version (returns the applied set); rest are CREATE TABLE +
        # INSERT pairs. Only the SELECT needs a non-trivial body.
        if "SELECT version FROM schema_migrations" in data.decode():
            return _FakeResponse(response_body)
        return _FakeResponse(b"")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    applied = migrate.apply_pending(host="ch", port=8123, db="eveys_ocpp")

    assert applied == [3, 4, 5, 6, 7]

    # Expected sequence: CREATE DATABASE → SELECT applied → for each
    # pending migration: CREATE TABLE + INSERT tracking row. Exactly
    # 2 + (N * 2) calls where N is the number of pending migrations.
    assert len(sqls_executed) == 2 + (5 * 2)

    # The very first call is the CREATE DATABASE (db=None).
    assert sqls_executed[0][0] is None
    assert "CREATE DATABASE IF NOT EXISTS eveys_ocpp" in sqls_executed[0][1]

    # The second is the version-list query (db=eveys_ocpp).
    assert sqls_executed[1][0] == "eveys_ocpp"
    assert "SELECT version FROM schema_migrations" in sqls_executed[1][1]

    # The next 6 calls alternate CREATE TABLE / INSERT for versions 3, 4, 5.
    # Every one of them runs against the eveys_ocpp database — the
    # CREATE DATABASE bootstrap (sqls_executed[0]) is the only db=None call.
    for sql_db, _sql in sqls_executed[2:]:
        assert sql_db == "eveys_ocpp"
    create_calls = [s for (_db, s) in sqls_executed[2:] if "CREATE TABLE" in s]
    insert_calls = [s for (_db, s) in sqls_executed[2:] if "INSERT INTO schema_migrations" in s]
    assert len(create_calls) == 5
    assert len(insert_calls) == 5
    # Tracking inserts carry the right (version, name) pairs.
    assert "(3, 'create_cp_status')" in insert_calls[0]
    assert "(4, 'create_cp_boot')" in insert_calls[1]
    assert "(5, 'create_tx_started')" in insert_calls[2]
    assert "(6, 'create_cp_offline_duration')" in insert_calls[3]
    assert "(7, 'create_cp_ocpp_frames')" in insert_calls[4]


def test_apply_pending_is_a_noop_when_everything_already_applied(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Idempotent: re-running against a fully-up-to-date ClickHouse
    only does the CREATE DATABASE + SELECT, no new INSERTs or
    CREATE TABLEs."""
    sqls_executed: list[str] = []
    # All applied versions present — including the new 0007 from
    # this PR.
    response_body = b"1\n2\n3\n4\n5\n6\n7\n"

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        sql = req.data.decode()  # type: ignore[attr-defined]
        sqls_executed.append(sql)
        if "SELECT version FROM schema_migrations" in sql:
            return _FakeResponse(response_body)
        return _FakeResponse(b"")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    applied = migrate.apply_pending(host="ch", port=8123, db="eveys_ocpp")

    assert applied == []
    # Just CREATE DATABASE + SELECT. No CREATE TABLE, no INSERT.
    assert len(sqls_executed) == 2
    assert "CREATE DATABASE" in sqls_executed[0]
    assert "SELECT version" in sqls_executed[1]


def test_apply_pending_handles_empty_ddl_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: object
) -> None:
    """Defensive: an empty ddl/ directory shouldn't crash. Returns
    an empty list and doesn't even hit the network."""
    from pathlib import Path

    fake_ddl = Path(str(tmp_path)) / "empty_ddl"
    fake_ddl.mkdir()
    monkeypatch.setattr(migrate, "_DDL_DIR", fake_ddl)

    # If urlopen *was* called, this would raise — guarantees we're not
    # making a request when there's nothing to do.
    monkeypatch.setattr(
        migrate.urllib.request,
        "urlopen",
        MagicMock(side_effect=AssertionError("should not call urlopen")),
    )

    applied = migrate.apply_pending(host="ch", port=8123, db="eveys_ocpp")
    assert applied == []


# ---- _record_applied SQL escaping -----------------------------------------


def test_record_applied_escapes_single_quotes_in_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The name comes from a filename match, so apostrophes are
    extremely unlikely — but the escape guard exists to prevent SQL
    injection if someone ever names a migration with one."""
    captured: dict[str, str] = {}

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        captured["sql"] = req.data.decode()  # type: ignore[attr-defined]
        return _FakeResponse(b"")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    migrate._record_applied(host="ch", port=8123, db="eveys_ocpp", version=42, name="o'malley")

    # SQL string literal: single quotes are doubled.
    assert "'o''malley'" in captured["sql"]


# ---- main() CLI entry point -----------------------------------------------


def test_main_exits_nonzero_when_clickhouse_unreachable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Network failure → clean error to stderr + exit 1, not a
    traceback."""

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        raise urllib.error.URLError("Connection refused")

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        "sys.argv",
        ["migrate.py", "--host", "nope", "--port", "8123", "--db", "eveys_ocpp"],
    )

    with pytest.raises(SystemExit) as excinfo:
        migrate.main()
    assert excinfo.value.code == 1

    err = capsys.readouterr().err
    assert "could not reach ClickHouse" in err
    assert "nope:8123" in err


# ---- logging: reserved LogRecord attribute regression ---------------------


def test_apply_pending_does_not_collide_with_reserved_logrecord_keys(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`logger.info("msg", extra={...})` raises KeyError if any extra
    key collides with a `LogRecord` reserved attribute (`name`, `msg`,
    `args`, `levelname`, etc). Pytest's default config has no handler
    attached so the bug is silently bypassed; CI's `make ch-migrate`
    calls `logging.basicConfig(...)` first, attaches a handler, and
    crashes on the next migration log line.

    Reproduce that path: attach a real handler before calling
    `apply_pending`, ensure it walks through at least one pending
    migration, and assert no exception escapes.
    """
    import logging

    sqls_executed: list[tuple[str | None, str]] = []

    def fake_urlopen(req: object, timeout: int = 0) -> _FakeResponse:
        url = req.full_url  # type: ignore[attr-defined]
        data = req.data  # type: ignore[attr-defined]
        db = url.split("?database=", 1)[1] if "?database=" in url else None
        sqls_executed.append((db, data.decode()))
        return _FakeResponse(b"")  # nothing applied yet — every migration is pending

    monkeypatch.setattr(migrate.urllib.request, "urlopen", fake_urlopen)

    # Attach a handler to migrate's logger — this is what triggers the
    # `LogRecord` reserved-key check at log time.
    handler = logging.StreamHandler(io.StringIO())
    handler.setLevel(logging.INFO)
    migrate.logger.addHandler(handler)
    migrate.logger.setLevel(logging.INFO)
    try:
        # If the `extra={"name": ...}` regression returns, this raises
        # `KeyError: "Attempt to overwrite 'name' in LogRecord"`.
        applied = migrate.apply_pending(host="ch", port=8123, db="eveys_ocpp")
    finally:
        migrate.logger.removeHandler(handler)

    assert applied  # at least one migration ran → the log path was exercised
