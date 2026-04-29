"""Smoke test that the package imports."""

from __future__ import annotations

import eveys_ocpp


def test_version_is_set() -> None:
    assert eveys_ocpp.__version__ == "0.0.0"
