"""One virtual charger — owns one WS connection and an FSM.

Lifecycle (per `BehaviourProfile`):

    DISCONNECTED → CONNECTING → BOOTED → IDLE ⇄ IN_SESSION → IDLE → …

Decisions ("start a transaction now?", "disconnect now?") are made
each tick by sampling the profile's per-minute probabilities. The
tick interval is short (1s) so cadences with sub-minute resolution
remain accurate without burning the event loop.

The charger publishes lightweight `ChargerStats` snapshots on a
shared `Counters` so the `Fleet` can render a live status line
without each charger broadcasting events.
"""

from __future__ import annotations

import asyncio
import contextlib
import random
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from ocpp.v16 import ChargePoint as Cp
from ocpp.v16 import call
from websockets.asyncio.client import connect

if TYPE_CHECKING:
    from tools.sim.profiles import BehaviourProfile


_OCPP_SUBPROTOCOL = "ocpp1.6"
_TICK_INTERVAL_SECONDS = 1.0


@dataclass(slots=True)
class Counters:
    """Shared aggregate the Fleet reads each second to render a status
    line. Each VirtualCharger holds a reference and bumps the relevant
    field on state changes; reads are racy-but-accurate-enough."""

    connected: int = 0
    boots: int = 0
    transactions: int = 0
    errors: int = 0


@dataclass(slots=True)
class _SimChargePoint(Cp):
    """python-ocpp client. The library handles framing + correlation."""


@dataclass(slots=True)
class VirtualCharger:
    cp_id: str
    target_url: str  # `ws://host:port` — `/cp_id` appended at connect
    profile: BehaviourProfile
    counters: Counters
    vendor: str = "EveysSim"
    model: str = "VirtualCharger"
    rng: random.Random = field(default_factory=random.Random)

    # Mutable runtime state — kept inside the dataclass so each
    # charger instance is fully self-contained.
    _id_tag: str = "SIM_RFID"
    _transaction_id: int | None = None
    _session_ends_at: float | None = None
    _next_heartbeat_at: float = 0.0
    _next_meter_at: float = 0.0
    _meter_value_wh: int = 0

    async def run(self) -> None:
        """Run forever (caller cancels). Reconnect-on-drop is part of
        the realistic and churning profiles."""
        while True:
            try:
                await self._one_session()
            except asyncio.CancelledError:
                return
            except Exception:
                # Don't let one charger's hiccup take down the whole fleet.
                # Bump errors so the live status reflects reality, then
                # sleep briefly before reconnecting.
                self.counters.errors += 1
                await asyncio.sleep(self.rng.uniform(0.5, 2.0))

    async def _one_session(self) -> None:
        """One connect → boot → run-until-disconnect cycle. The outer
        loop reconnects after this returns."""
        url = f"{self.target_url.rstrip('/')}/{self.cp_id}"
        async with connect(url, subprotocols=[_OCPP_SUBPROTOCOL]) as ws:
            cp = _SimChargePoint(self.cp_id, ws)
            recv_task = asyncio.create_task(cp.start(), name=f"sim-recv-{self.cp_id}")
            self.counters.connected += 1
            try:
                await self._boot(cp)
                await self._tick_loop(cp)
            finally:
                recv_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await recv_task
                self.counters.connected -= 1

    async def _boot(self, cp: _SimChargePoint) -> None:
        result = await cp.call(
            call.BootNotification(charge_point_vendor=self.vendor, charge_point_model=self.model)
        )
        if getattr(result, "status", None) != "Accepted":
            # Gateway rejected the boot — bail out of this session and
            # let the outer loop reconnect. Counts as an error.
            raise RuntimeError(f"boot rejected: {getattr(result, 'status', '?')}")
        self.counters.boots += 1
        # Honour the gateway-requested heartbeat interval if provided.
        interval = float(getattr(result, "interval", 0) or self.profile.heartbeat_period_seconds)
        loop = asyncio.get_running_loop()
        self._next_heartbeat_at = loop.time() + interval

    async def _tick_loop(self, cp: _SimChargePoint) -> None:
        """One pass per `_TICK_INTERVAL_SECONDS`. Fires whichever
        time-driven actions are due, then samples the per-minute
        probabilities for non-time-driven actions (transaction start,
        disconnect)."""
        loop = asyncio.get_running_loop()
        # Per-tick probability = (per-minute / 60) since each tick is 1s.
        per_tick_start = self.profile.transaction_start_per_minute / 60.0
        per_tick_disconnect = self.profile.disconnect_per_minute / 60.0
        while True:
            await asyncio.sleep(_TICK_INTERVAL_SECONDS)
            now = loop.time()

            # Heartbeat — always when interval elapses.
            if now >= self._next_heartbeat_at:
                await cp.call(call.Heartbeat())
                self._next_heartbeat_at = now + self.profile.heartbeat_period_seconds

            # If in-session: maybe send a MeterValue, maybe stop.
            if self._transaction_id is not None:
                if self.profile.meter_values_period_seconds > 0 and now >= self._next_meter_at:
                    await self._send_meter_value(cp)
                    self._next_meter_at = now + self.profile.meter_values_period_seconds
                if self._session_ends_at is not None and now >= self._session_ends_at:
                    await self._stop_transaction(cp)
                continue

            # Idle: maybe start a transaction this tick.
            if per_tick_start > 0 and self.rng.random() < per_tick_start:
                await self._start_transaction(cp, now)

            # Idle: maybe drop the connection (forces a reconnect).
            if per_tick_disconnect > 0 and self.rng.random() < per_tick_disconnect:
                # Closing the WS bubbles up as ConnectionClosed inside
                # the `_SimChargePoint.start()` task — the outer
                # `_one_session` finally cleans up.
                await cp._connection.close()
                return

    async def _start_transaction(self, cp: _SimChargePoint, now: float) -> None:
        # Authorize first so we exercise the same code path as a real
        # charger. Treat any non-Accepted as a soft failure.
        auth = await cp.call(call.Authorize(id_tag=self._id_tag))
        status = (auth.id_tag_info or {}).get("status") if auth.id_tag_info else None
        if status != "Accepted":
            return
        result = await cp.call(
            call.StartTransaction(
                connector_id=1,
                id_tag=self._id_tag,
                meter_start=self._meter_value_wh,
                timestamp=datetime.now(UTC).isoformat(),
            )
        )
        tx_id = int(getattr(result, "transaction_id", 0))
        if tx_id <= 0:
            return
        self._transaction_id = tx_id
        # Exponential session length around the configured mean keeps
        # the simulated load shape close to a real fleet.
        self._session_ends_at = now + self.rng.expovariate(
            1.0 / max(self.profile.session_length_seconds_mean, 1.0)
        )
        self._next_meter_at = now + self.profile.meter_values_period_seconds
        self.counters.transactions += 1

    async def _send_meter_value(self, cp: _SimChargePoint) -> None:
        # Add a plausible 100Wh chunk per sample (≈12kW for a 30s window).
        self._meter_value_wh += 100
        await cp.call(
            call.MeterValues(
                connector_id=1,
                transaction_id=self._transaction_id,
                meter_value=[
                    {
                        "timestamp": datetime.now(UTC).isoformat(),
                        "sampled_value": [
                            {
                                "value": str(self._meter_value_wh),
                                "context": "Sample.Periodic",
                                "format": "Raw",
                                "measurand": "Energy.Active.Import.Register",
                                "unit": "Wh",
                            }
                        ],
                    }
                ],
            )
        )

    async def _stop_transaction(self, cp: _SimChargePoint) -> None:
        await cp.call(
            call.StopTransaction(
                meter_stop=self._meter_value_wh,
                timestamp=datetime.now(UTC).isoformat(),
                transaction_id=self._transaction_id or 0,
                reason="Local",
                id_tag=self._id_tag,
            )
        )
        self._transaction_id = None
        self._session_ends_at = None
