"""ClickHouse schema migrator (E2-13, ADR-0020).

Reads SQL files under ``src/eveys_ocpp/clickhouse/ddl/``, checks which
ones have already been applied (via the ``schema_migrations`` table),
runs the pending ones in version order, and records each one as
applied.

Why this is its own tiny script and not a third-party migration tool
(see ADR-0020 § "Migration tooling"): the contract is small, plain
SQL is the language ClickHouse actually speaks, and adding a
migration framework just to manage 5 DDL files is the kind of
premature abstraction the project standards reject.

Why HTTP `urllib` and not the async `asynch` driver: the migrator is
a one-shot CLI invoked by `make ch-migrate` or by the compose
init-container. No reason to spin up an asyncio loop; sync HTTP is
the simplest possible client and adds no runtime dep beyond the
stdlib. The ingestor (which is genuinely on the asyncio hot path)
uses `asynch`; the two paths use different drivers on purpose.

Filename convention:
    ``NNNN_short_snake_case.sql`` — 4-digit version prefix (1-padded),
    underscore, descriptive suffix. The version is parsed from the
    prefix and stored in ``schema_migrations.version``. Files are
    applied strictly in version order; never edit a merged file.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import urllib.parse
import urllib.request
from pathlib import Path

logger = logging.getLogger(__name__)

_DDL_DIR = Path(__file__).parent / "ddl"
_FILENAME_RE = re.compile(r"^(\d{4})_([a-z0-9_]+)\.sql$")


def _read_migrations() -> list[tuple[int, str, str]]:
    """Return ``[(version, name, sql), ...]`` sorted by version."""
    out: list[tuple[int, str, str]] = []
    for path in sorted(_DDL_DIR.iterdir()):
        match = _FILENAME_RE.match(path.name)
        if not match:
            continue  # ignore stray files; convention-driven
        version = int(match.group(1))
        name = match.group(2)
        sql = path.read_text(encoding="utf-8")
        out.append((version, name, sql))
    return out


def _execute(host: str, port: int, db: str | None, sql: str) -> str:
    """Run a single SQL statement against ClickHouse over HTTP.

    Returns the response body as a string. Raises if the HTTP call
    fails or ClickHouse returns a non-2xx status — callers treat any
    error as fatal (no migration partial-apply recovery).

    ``db=None`` runs the statement without a default database — used
    by ``_ensure_database`` to ``CREATE DATABASE`` before any other
    statement (which would 404 if the database doesn't exist yet).
    """
    query: dict[str, str] = {}
    if db is not None:
        query["database"] = db
    suffix = f"?{urllib.parse.urlencode(query)}" if query else ""
    url = f"http://{host}:{port}/{suffix}"
    req = urllib.request.Request(
        url,
        data=sql.encode("utf-8"),
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body: str = resp.read().decode("utf-8")
    return body


def _ensure_database(host: str, port: int, db: str) -> None:
    """Create the database if it doesn't exist.

    The migrator is expected to be invoked against either compose
    (where `CLICKHOUSE_DB=eveys_ocpp` ensures the DB at container init)
    OR a fresh local ClickHouse install where the operator hasn't
    pre-created the database. The compose case is a no-op; the
    fresh-install case becomes a clean first run.
    """
    _execute(host, port, None, f"CREATE DATABASE IF NOT EXISTS {db}")


def _applied_versions(host: str, port: int, db: str) -> set[int]:
    """Return the set of migration versions already applied.

    Returns the empty set if `schema_migrations` doesn't exist yet
    (the bootstrap case — the very first migration creates it).
    """
    try:
        body = _execute(host, port, db, "SELECT version FROM schema_migrations FORMAT TabSeparated")
    except urllib.error.HTTPError as exc:
        # ClickHouse error code 60 = UNKNOWN_TABLE. First-run bootstrap.
        if b"UNKNOWN_TABLE" in (exc.read() or b""):
            return set()
        raise
    return {int(line) for line in body.splitlines() if line.strip()}


def _record_applied(host: str, port: int, db: str, version: int, name: str) -> None:
    """Insert a row into `schema_migrations` for a successfully-applied
    migration. Done after the DDL itself succeeds — if the DDL fails the
    row is never written and the next run retries the same migration."""
    sql = (
        f"INSERT INTO schema_migrations (version, name) VALUES "
        f"({version}, '{name.replace(chr(39), chr(39) + chr(39))}')"
    )
    _execute(host, port, db, sql)


def apply_pending(*, host: str, port: int, db: str) -> list[int]:
    """Apply every migration whose version is not yet in
    `schema_migrations`. Returns the list of versions newly applied
    (empty if everything was already up-to-date)."""
    migrations = _read_migrations()
    if not migrations:
        logger.warning("clickhouse.migrate.no_ddl_files_found")
        return []

    # Bootstrap step: make sure the database exists. Idempotent
    # (`CREATE DATABASE IF NOT EXISTS`); a no-op against a
    # compose-initialised ClickHouse, but lets `make ch-migrate`
    # work against a fresh local install too.
    _ensure_database(host, port, db)

    applied = _applied_versions(host=host, port=port, db=db)
    pending = [(v, n, s) for (v, n, s) in migrations if v not in applied]
    newly_applied: list[int] = []

    for version, name, sql in pending:
        # `extra` keys must not collide with `LogRecord`'s reserved
        # attribute names — `name` is the logger name, so we prefix
        # the migration's identifying fields. Without the prefix
        # stdlib `logging` raises `KeyError: "Attempt to overwrite
        # 'name' in LogRecord"`.
        logger.info(
            "clickhouse.migrate.applying",
            extra={"migration_version": version, "migration_name": name},
        )
        # Apply the DDL first; only then record it as applied. If the
        # DDL fails, the record step is skipped and the next run
        # retries the same migration. The first migration creates
        # `schema_migrations` itself — by the time we record below,
        # the table exists.
        _execute(host, port, db, sql)
        _record_applied(host=host, port=port, db=db, version=version, name=name)
        newly_applied.append(version)

    if newly_applied:
        logger.info(
            "clickhouse.migrate.applied",
            extra={"versions": newly_applied},
        )
    else:
        logger.info("clickhouse.migrate.up_to_date")
    return newly_applied


def main() -> None:
    """CLI entry point: ``python -m eveys_ocpp.clickhouse.migrate``."""
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser(description="Apply ClickHouse migrations.")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=8123, help="ClickHouse HTTP port")
    parser.add_argument("--db", default="eveys_ocpp")
    args = parser.parse_args()

    try:
        applied = apply_pending(host=args.host, port=args.port, db=args.db)
    except urllib.error.URLError as exc:
        print(
            f"ERROR: could not reach ClickHouse at {args.host}:{args.port}: {exc}",
            file=sys.stderr,
        )
        sys.exit(1)
    print(f"applied {len(applied)} migration(s): {applied}")


if __name__ == "__main__":
    main()
