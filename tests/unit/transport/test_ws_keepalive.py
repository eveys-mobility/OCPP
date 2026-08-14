"""Server-side WebSocket keepalive wiring.

The bug this guards against is a wiring gap, not a buggy function.
`serve_forever` used to call `websockets.asyncio.server.serve()` without
`ping_interval` / `ping_timeout`, so the library defaults (20 s / 20 s)
applied: the gateway pinged every 20 s and closed any connection whose
pong had not arrived 20 s later.

Chargers on high-latency cellular links could not make that deadline.
Their sessions died at exactly 40.01 s or 60.01 s — machine-precise
multiples of the 20 s ping cycle — and in production five units sat in a
permanent reconnect loop: 185 disconnects an hour against ~30 chargers,
176 of them logged `keepalive ping timeout`, leaving those chargers
offline roughly 40 % of the time so field remote-starts failed.

No unit test could have caught that, because every function involved
worked correctly in isolation. So these tests assert the call itself:
that `serve()` receives the configured values, and — the actual
regression test — that the defaults are *not* the library's 20/20.

`serve()` is patched rather than bound to a real socket because the
kwargs are what is under test; a live listener cannot observe them.
"""

from __future__ import annotations

from typing import Any, cast

import pytest

from eveys_ocpp.settings import Settings
from eveys_ocpp.transport.ws_server import serve_forever


class _StopServing(Exception):
    """Sentinel raised from the fake `serve_forever()` to unwind."""


def _fake_serve_capturing(captured: dict[str, Any]) -> type:
    """Build a stand-in for `serve` that records its kwargs.

    `serve` is used as `async with serve(...) as server:` followed by
    `await server.serve_forever()`, so the fake has to be an async
    context manager whose `__aenter__` yields something with a
    `serve_forever`. That inner call raises `_StopServing` so the
    function under test returns instead of blocking forever.
    """

    class _FakeServe:
        def __init__(self, handler: Any, **kwargs: Any) -> None:
            captured["kwargs"] = kwargs

        async def __aenter__(self) -> Any:
            class _Server:
                async def serve_forever(_self) -> None:
                    raise _StopServing

            return _Server()

        async def __aexit__(self, *exc: Any) -> bool:
            return False

    return _FakeServe


async def _capture_serve_kwargs(
    monkeypatch: pytest.MonkeyPatch, settings: Settings
) -> dict[str, Any]:
    """Run `serve_forever` against a patched `serve` and return its kwargs."""
    captured: dict[str, Any] = {}
    monkeypatch.setattr("eveys_ocpp.transport.ws_server.serve", _fake_serve_capturing(captured))
    with pytest.raises(_StopServing):
        await serve_forever(
            # Only `settings` is read before `serve()` is reached; the
            # rest are captured by closures that never run here.
            session_factory=cast(Any, None),
            settings=settings,
            pending_store=cast(Any, object()),
        )
    return cast(dict[str, Any], captured["kwargs"])


@pytest.mark.asyncio
async def test_serve_forever_passes_configured_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Operator-set values reach `serve()` unchanged."""
    kwargs = await _capture_serve_kwargs(
        monkeypatch,
        Settings(
            ws_keepalive_ping_interval_seconds=45,
            ws_keepalive_ping_timeout_seconds=120,
            ws_keepalive_close_timeout_seconds=7,
        ),
    )

    assert kwargs["ping_interval"] == 45
    assert kwargs["ping_timeout"] == 120
    assert kwargs["close_timeout"] == 7


@pytest.mark.asyncio
async def test_serve_forever_defaults_are_not_the_library_defaults(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The regression test: defaults must not fall back to 20 s / 20 s.

    If someone deletes the `ping_interval=` / `ping_timeout=` kwargs,
    `serve()` silently reverts to the library defaults that caused the
    incident. This fails loudly instead.
    """
    kwargs = await _capture_serve_kwargs(monkeypatch, Settings())

    assert kwargs["ping_interval"] == 30
    assert kwargs["ping_timeout"] == 30
    assert (kwargs["ping_interval"], kwargs["ping_timeout"]) != (20, 20)


@pytest.mark.asyncio
async def test_zero_disables_server_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`0` is the escape hatch: it maps to `None`, not to a 0 s timer.

    Reserved for firmware that never answers ping frames at all, where
    raising the timeout only moves the disconnect rather than fixing it.
    """
    kwargs = await _capture_serve_kwargs(
        monkeypatch,
        Settings(
            ws_keepalive_ping_interval_seconds=0,
            ws_keepalive_ping_timeout_seconds=0,
        ),
    )

    assert kwargs["ping_interval"] is None
    assert kwargs["ping_timeout"] is None
