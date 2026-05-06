"""Unit tests for the in-process ConnectionMap (E2-5)."""

from __future__ import annotations

from unittest.mock import MagicMock

from eveys_ocpp.connections import ConnectionMap


def _fake_cp(cp_id: str) -> MagicMock:
    cp = MagicMock()
    cp.id = cp_id
    return cp


def test_add_then_get() -> None:
    m = ConnectionMap()
    cp = _fake_cp("CP_001")
    m.add(cp)
    assert m.get("CP_001") is cp


def test_get_returns_none_for_unknown() -> None:
    m = ConnectionMap()
    assert m.get("UNKNOWN") is None


def test_contains_operator() -> None:
    m = ConnectionMap()
    m.add(_fake_cp("CP_001"))
    assert "CP_001" in m
    assert "NOPE" not in m


def test_len_tracks_active_connections() -> None:
    m = ConnectionMap()
    assert len(m) == 0
    m.add(_fake_cp("CP_A"))
    m.add(_fake_cp("CP_B"))
    assert len(m) == 2


def test_remove_when_owner_returns_true() -> None:
    m = ConnectionMap()
    cp = _fake_cp("CP_001")
    m.add(cp)
    assert m.remove(cp) is True
    assert m.get("CP_001") is None


def test_remove_when_not_owner_returns_false() -> None:
    """Reconnect race — newer connection took ownership before we got
    here. The older connection's finally block must not delete the new one.
    """
    m = ConnectionMap()
    old_cp = _fake_cp("CP_001")
    new_cp = _fake_cp("CP_001")
    m.add(old_cp)
    m.add(new_cp)  # replaces old_cp

    assert m.remove(old_cp) is False
    assert m.get("CP_001") is new_cp  # new_cp still there


def test_add_logs_replacement_warning() -> None:
    """add() of a second cp for the same id is allowed — older WS is
    presumably half-dead. Just don't lose the new one.
    """
    m = ConnectionMap()
    m.add(_fake_cp("CP_001"))
    new_cp = _fake_cp("CP_001")
    m.add(new_cp)
    assert m.get("CP_001") is new_cp
