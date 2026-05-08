"""Tests for the `DrainController` lifecycle flag."""

from __future__ import annotations

import time

from eveys_ocpp.shutdown import DrainController


def test_drain_controller_starts_not_draining() -> None:
    controller = DrainController()

    assert controller.is_draining is False
    assert controller.drain_started_at is None


def test_begin_drain_flips_flag_and_records_timestamp() -> None:
    controller = DrainController()
    before = time.monotonic()

    controller.begin_drain()

    assert controller.is_draining is True
    assert controller.drain_started_at is not None
    assert controller.drain_started_at >= before


def test_begin_drain_is_idempotent() -> None:
    """A second SIGTERM (or programmatic re-trigger) must not reset
    the original timestamp. Operators rely on `drain_started_at` to
    know how long the pod has been draining."""
    controller = DrainController()

    controller.begin_drain()
    first_ts = controller.drain_started_at

    # Wait long enough that a re-stamping bug would be visible to a
    # monotonic clock (microseconds is plenty).
    time.sleep(0.001)
    controller.begin_drain()

    assert controller.drain_started_at == first_ts
    assert controller.is_draining is True
