#!/usr/bin/env python3
"""Compare the wide metric-log merge with the configured policy on synthetic data.

Run with an explicit official ClickHouse binary under the OS deny-network sandbox.
No server, socket, production configuration or application bootstrap is used.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile
import xml.etree.ElementTree as ET


def run(binary: Path, query: str) -> subprocess.CompletedProcess[str]:
    with tempfile.TemporaryDirectory(prefix="ocpp-metric-memory-") as directory:
        try:
            return subprocess.run(
                [str(binary), "local", "--max_memory_usage=268435456", "--max_threads=2",
                 "--max_block_size=1024", "--multiquery", "--query", query],
                cwd=directory, env={"PATH": "/usr/bin:/bin", "TZ": "UTC"},
                text=True, capture_output=True, timeout=45, check=False,
            )
        except subprocess.TimeoutExpired:
            raise SystemExit("Synthetic ClickHouse operation exceeded 45 seconds") from None


def table(settings: str = "") -> str:
    columns = ", ".join(f"m{i} UInt64" for i in range(1100))
    clause = f" SETTINGS {settings}" if settings else ""
    return f"CREATE TABLE synthetic_metric_log (event_date Date, event_time DateTime, {columns}) " \
           f"ENGINE=MergeTree ORDER BY event_time{clause};"


def operations() -> str:
    values = ", ".join(f"toUInt64(number+{i})" for i in range(1100))
    return f"""
INSERT INTO synthetic_metric_log SELECT today(), now(), {values} FROM numbers(1024);
INSERT INTO synthetic_metric_log SELECT today(), now(), {values} FROM numbers(1024);
OPTIMIZE TABLE synthetic_metric_log FINAL;
SELECT count(), sum(m0), sum(m1099) FROM synthetic_metric_log;
SELECT part_type, rows FROM system.parts WHERE table='synthetic_metric_log' AND active;
"""


def scenario(settings: str) -> str:
    return table(settings) + operations()


def reviewed_statement(path: Path, operation: str) -> str:
    """Validate the manual artifact, then change only its target in isolation."""
    sql = "\n".join(line for line in path.read_text().splitlines()
                    if not line.lstrip().startswith("--")).strip()
    policy = (r"min_bytes_for_wide_part\s*=\s*18446744073709551615\s*,\s*"
              r"merge_max_block_size\s*=\s*1024") if operation == "MODIFY" else (
              r"min_bytes_for_wide_part\s*,\s*merge_max_block_size")
    if not re.fullmatch(rf"ALTER\s+TABLE\s+system\.metric_log\s+{operation}\s+SETTING\s+{policy}\s*;", sql):
        raise SystemExit(f"Unexpected manual metric-log statement: {path.name}")
    return sql.replace("system.metric_log", "synthetic_metric_log", 1)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--config", type=Path, default=Path(__file__).resolve().parents[1] /
                        "deploy/compose/clickhouse/config.d/zz-eveys-prod.xml")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    version = run(binary, "SELECT version()")
    if version.returncode or version.stdout.strip() != "24.10.2.80":
        raise SystemExit("Expected the explicit audited ClickHouse24.10.2.80 binary")

    # Exercise the Wide writer explicitly. This reproduces the memory mechanism,
    # not the production merge selector or its data/part distribution.
    baseline = run(binary, scenario("min_bytes_for_wide_part=0, min_rows_for_wide_part=0"))
    if baseline.returncode != 241 or "MEMORY_LIMIT_EXCEEDED" not in baseline.stderr:
        raise SystemExit(f"Baseline did not reproduce Code241 (exit={baseline.returncode})")

    root = ET.parse(args.config).getroot()
    metric = root.find("metric_log")
    settings = metric.findtext("settings", "") if metric is not None else ""
    # Only this finite numeric table policy can enter the local SQL statement.
    if not re.fullmatch(r"\s*[a-z_]+\s*=\s*\d+(?:\s*,\s*[a-z_]+\s*=\s*\d+)*\s*", settings):
        raise SystemExit("Metric-log memory policy missing or nonnumeric")
    candidate = run(binary, scenario(settings))
    expected = "2048\t1047552\t3298304\nCompact\t2048"
    if candidate.returncode or candidate.stdout.strip() != expected:
        raise SystemExit(f"Configured policy failed (exit={candidate.returncode}): {candidate.stderr[-600:]}")
    # Existing-table DDL and rollback are separate deployment artifacts. Execute
    # both against the synthetic table; the real system table is never changed.
    directory = args.config.resolve().parents[1]
    apply_sql = reviewed_statement(directory / "metric-log-memory-policy.sql", "MODIFY")
    rollback_sql = reviewed_statement(directory / "metric-log-memory-policy.rollback.sql", "RESET")
    no_overrides = """
SELECT position(engine_full, 'min_bytes_for_wide_part')=0
   AND position(engine_full, 'merge_max_block_size')=0
FROM system.tables WHERE name='synthetic_metric_log';
"""
    after_reset = "SELECT count(), sum(m0), sum(m1099) FROM synthetic_metric_log;"
    roundtrip = run(binary, table() + no_overrides + apply_sql + operations() + rollback_sql +
                    no_overrides + after_reset)
    if roundtrip.returncode or roundtrip.stdout.strip() != f"1\n{expected}\n1\n2048\t1047552\t3298304":
        raise SystemExit(f"Existing-table ALTER/RESET failed (exit={roundtrip.returncode}): "
                         f"{roundtrip.stderr[-600:]}")
    print(json.dumps({
        "version": version.stdout.strip(), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
        "columns": 1102, "rows": 2048, "query_memory_limit_bytes": 268435456,
        "baseline": {"exit": baseline.returncode, "error": "MEMORY_LIMIT_EXCEEDED"},
        "configured": {"exit": candidate.returncode, "part_type": "Compact", "merge_and_sums_verified": True},
        "existing_table": {"alter_and_reset_verified": True, "rows_and_sums_preserved": True},
        "scope": "isolated synthetic native engine; not production restore/load proof",
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
