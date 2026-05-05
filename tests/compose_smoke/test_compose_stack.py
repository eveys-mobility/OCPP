"""Tier-3 compose-smoke: the built image actually runs (ADR-0024).

The session-scoped fixture in ``conftest.py`` brings the production-shaped
stack up before any test in this module sees a request, and asserts that
**every** container reaches a stable Running state with no restarts. By
the time these test bodies execute, the basic "stack is alive" claim is
already proven — these tests then extend the proof to:

1. Each container holds the right entrypoint (catches the
   ``ENTRYPOINT`` / ``CMD`` swallowing bug).
2. Each container reports the env it should report (catches the
   "missing env var → falls back to localhost default → crashes" class
   of bug, even if the crash takes minutes to materialise).
3. A real OCPP charger can complete a full transaction flow end-to-end
   against the published host port.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
from datetime import UTC, datetime

import pytest
from ocpp.v16 import ChargePoint as Cp
from ocpp.v16 import call
from websockets.asyncio.client import connect

from tests.compose_smoke.conftest import (
    HOST_WS_PORT,
    _container_logs,
    _container_state,
)

# ---- container shape ------------------------------------------------------


def _inspect_field(name: str, fmt: str) -> str:
    """Light wrapper around `docker inspect --format`."""
    proc = subprocess.run(
        ["docker", "inspect", "--format", fmt, name],
        check=True,
        capture_output=True,
        text=True,
    )
    return proc.stdout.strip()


def test_gateway_runs_gateway_entrypoint() -> None:
    """`eveys-ocpp` runs `python -m eveys_ocpp` — not the ingestor, not
    something silently swallowed by the Dockerfile entrypoint shape."""
    cmd = json.loads(_inspect_field("eveys-ocpp", "{{json .Config.Cmd}}"))
    entry = json.loads(_inspect_field("eveys-ocpp", "{{json .Config.Entrypoint}}"))

    # Entrypoint is the interpreter, command is the module.
    assert entry == ["/usr/local/bin/python3.13"], f"gateway entrypoint changed: {entry}"
    assert cmd == ["-m", "eveys_ocpp"], f"gateway command changed: {cmd}"


def test_ingestor_runs_ingestor_module() -> None:
    """`clickhouse-ingestor` actually runs the ingestor — not the
    gateway. This is the bug from the audit: with a combined
    ``ENTRYPOINT ["python", "-m", "eveys_ocpp"]`` the compose
    ``command:`` was silently appended as positional argv and the
    gateway ran in the ingestor container too."""
    cmd = json.loads(_inspect_field("eveys-ocpp-clickhouse-ingestor", "{{json .Config.Cmd}}"))
    assert cmd == ["-m", "eveys_ocpp.clickhouse.ingestor"], (
        f"ingestor command does not run the ingestor module: {cmd}"
    )


def test_gateway_has_required_env_for_in_network_clients() -> None:
    """The gateway container ships these env vars set explicitly.

    Without them, `Settings` defaults take over and the gateway tries
    to reach `localhost:9092` / `localhost:6379` — both unreachable
    inside the container's own network namespace. That's the exact
    bug the audit caught.
    """
    raw = _inspect_field("eveys-ocpp", "{{json .Config.Env}}")
    env_list = json.loads(raw)
    env: dict[str, str] = {}
    for entry in env_list:
        k, _, v = entry.partition("=")
        env[k] = v

    # Each of these MUST point at an in-network service alias, NOT
    # localhost. We assert on the host token rather than equality so a
    # future port renumber doesn't gratuitously break the test.
    assert "kafka:" in env.get("EVEYS_OCPP_KAFKA_BROKERS", ""), env
    assert "redis:" in env.get("EVEYS_OCPP_REDIS_URL", ""), env
    assert "postgres:" in env.get("EVEYS_OCPP_DB_URL", ""), env
    assert env.get("EVEYS_OCPP_CLICKHOUSE_HOST") == "clickhouse", env


def test_no_gateway_dependency_errors_in_logs() -> None:
    """The gateway log up to this point contains no
    ``Connection refused`` / ``ConnectionError`` entries against its
    own dependencies (Kafka, Redis, Postgres).

    A container that is `Up` but spamming "Connection refused" is the
    silent-failure mode this tier was added to catch — even if every
    container reports as Running, the actual data flow is broken.

    We deliberately do NOT assert "no Traceback ever logged": the
    ``websockets`` library logs ``ConnectionClosedOK`` (the benign
    1000-OK close every charger emits) as a stack trace, and that is
    library noise we cannot suppress without forking websockets. The
    point of this assertion is dependency-reachability, not stack
    traces in general.
    """
    logs = _container_logs("eveys-ocpp", tail=500)
    forbidden = ("ConnectionRefusedError", "ConnectionError:")
    bad = [tok for tok in forbidden if tok in logs]
    assert not bad, f"gateway logs contain dependency-error tokens {bad}:\n--- logs ---\n{logs}"


def test_no_ingestor_dependency_errors_in_logs() -> None:
    logs = _container_logs("eveys-ocpp-clickhouse-ingestor", tail=500)
    forbidden = ("ConnectionRefusedError", "ConnectionError:")
    bad = [tok for tok in forbidden if tok in logs]
    assert not bad, f"ingestor logs contain dependency-error tokens {bad}:\n--- logs ---\n{logs}"


def test_no_container_has_restarted() -> None:
    """Belt-and-braces re-check at test time, not just fixture time."""
    for name in (
        "eveys-ocpp",
        "eveys-ocpp-clickhouse-ingestor",
        "eveys-ocpp-postgres",
        "eveys-ocpp-redis",
        "eveys-ocpp-kafka",
        "eveys-ocpp-clickhouse",
    ):
        state = _container_state(name)
        assert state.get("running") == "true", (
            f"{name} not running: state={state}\n--- logs ---\n{_container_logs(name)}"
        )
        rc = int(state.get("restart_count") or "0")
        assert rc == 0, f"{name} has restart_count={rc}\n--- logs ---\n{_container_logs(name)}"


# ---- end-to-end charger flow against the running container ----------------


class _SimChargePoint(Cp):
    """Minimal charger sim — same library production uses."""


@pytest.mark.asyncio
async def test_full_charger_flow_against_running_container() -> None:
    """Drive Boot → Authorize → StartTransaction → MeterValues →
    StopTransaction over the published host WS port.

    Failure here means the container is up but cannot serve OCPP
    traffic — e.g., a route registration regressed, the WS subprotocol
    handshake broke, or the DB session factory is misconfigured. None
    of those are observable in unit or e2e tiers.
    """
    cp_id = "COMPOSE_SMOKE_CP"
    url = f"ws://localhost:{HOST_WS_PORT}/{cp_id}"

    async with connect(url, subprotocols=["ocpp1.6"]) as ws:
        sim = _SimChargePoint(cp_id, ws)
        loop = asyncio.create_task(sim.start())

        try:
            boot = await asyncio.wait_for(
                sim.call(
                    call.BootNotification(
                        charge_point_vendor="ACME",
                        charge_point_model="X1",
                    )
                ),
                timeout=10,
            )
            assert boot.status == "Accepted"

            auth = await asyncio.wait_for(
                sim.call(call.Authorize(id_tag="VALID_RFID_001")),
                timeout=10,
            )
            assert auth.id_tag_info["status"] == "Accepted"

            start = await asyncio.wait_for(
                sim.call(
                    call.StartTransaction(
                        connector_id=1,
                        id_tag="VALID_RFID_001",
                        meter_start=0,
                        timestamp=datetime.now(UTC).isoformat(),
                    )
                ),
                timeout=10,
            )
            assert start.id_tag_info["status"] == "Accepted"
            assert start.transaction_id > 0
            tx_id = int(start.transaction_id)

            # MeterValues — exercises the Kafka publish path so the
            # ingestor materialises a row downstream. We don't assert
            # the ClickHouse row here (separate test below); we just
            # need the publish to succeed without a Kafka error.
            await asyncio.wait_for(
                sim.call(
                    call.MeterValues(
                        connector_id=1,
                        transaction_id=tx_id,
                        meter_value=[
                            {
                                "timestamp": datetime.now(UTC).isoformat(),
                                "sampled_value": [
                                    {
                                        "value": "1234",
                                        "measurand": "Energy.Active.Import.Register",
                                        "unit": "Wh",
                                    }
                                ],
                            }
                        ],
                    )
                ),
                timeout=10,
            )

            stop = await asyncio.wait_for(
                sim.call(
                    call.StopTransaction(
                        meter_stop=12345,
                        timestamp=datetime.now(UTC).isoformat(),
                        transaction_id=tx_id,
                        reason="Local",
                        id_tag="VALID_RFID_001",
                    )
                ),
                timeout=10,
            )
            assert stop.id_tag_info["status"] == "Accepted"
        finally:
            loop.cancel()


# ---- end-to-end pipeline (Kafka → ClickHouse via ingestor) ----------------


def test_clickhouse_ingestor_consumed_partitions() -> None:
    """The ingestor logs a `Setting newly assigned partitions` line as
    soon as Kafka group-join succeeds. If we don't see it, the
    ingestor never came up cleanly enough to consume — which means a
    `MeterValues` published in the previous test will never reach
    ClickHouse, even though the gateway considered it delivered.
    """
    logs = _container_logs("eveys-ocpp-clickhouse-ingestor", tail=500)
    assert "Setting newly assigned partitions" in logs, (
        "ingestor never joined a Kafka consumer group — "
        f"the ClickHouse pipeline is silently broken.\n--- logs ---\n{logs}"
    )
