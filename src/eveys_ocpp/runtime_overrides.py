"""Per-pod runtime overrides for a tightly-scoped set of Settings fields.

`Settings` is frozen (ADR-0001) — env vars are the source of truth and
most fields are read once at boot, baking the value into a SQLAlchemy
engine, an aiokafka producer, a TCP socket bind, etc. Mutating those
at runtime would either be a no-op or a bug.

A small subset of Settings fields **are** read fresh per call site,
which makes them safe to flip at runtime. This module is the
in-memory store for those overrides. Read sites that opt in consult
`get(name, default=settings.foo)` instead of `settings.foo` directly.

**Per-pod scope.** Hitting the admin endpoint on pod A doesn't
affect pod B. Cluster-wide propagation via Redis pub/sub is a
future enhancement; for v0 the rolling deploy with a new env value
is the canonical mechanism for fleet-wide config changes.

**In-memory only.** A pod restart reverts to env. Documented; no
persistence.

**Thread-safety.** A single `threading.Lock` around the dict is enough
for the API surface — writes are admin-rate (manual operator action),
reads are per-request but the GIL plus the lock are cheap. We don't
use `asyncio.Lock` because some read sites (instrumentation, logging)
aren't async.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from eveys_ocpp.observability import get_logger

log = get_logger(__name__)


# Closed allowlist of fields the admin endpoint will accept on PATCH.
# Adding a field is a deliberate decision: the call site has to be
# verified to read fresh on every use, *and* the value has to make
# sense to flip without a restart. New entries pair with a new
# `read_setting(...)` call at the matching read site.
@dataclass(frozen=True, slots=True)
class _AllowlistEntry:
    """One allowlisted field plus the validator that coerces an
    incoming JSON value to the right Python type. Validators keep the
    REST API tolerant — operators can send `"true"`, `True`, or `1`
    for a bool — without losing type discipline at the read site.
    """

    name: str
    coerce: Callable[[Any], Any]
    description: str


def _coerce_log_level(value: Any) -> str:
    """Validate against the same Literal as Settings.log_level."""
    allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if not isinstance(value, str) or value not in allowed:
        raise ValueError(f"log_level must be one of {sorted(allowed)}; got {value!r}")
    return value


def _coerce_bool(value: Any) -> bool:
    """Tolerant bool coercion. JSON, form-encoded clients, and
    operator-typed CLI all produce different shapes; reduce them to
    one Python bool."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        if value.lower() in ("true", "1", "yes", "on"):
            return True
        if value.lower() in ("false", "0", "no", "off"):
            return False
    if isinstance(value, int):
        return bool(value)
    raise ValueError(f"expected boolean, got {value!r}")


# The allowlist itself. See the module docstring for the criteria.
_ALLOWLIST: dict[str, _AllowlistEntry] = {
    "log_level": _AllowlistEntry(
        name="log_level",
        coerce=_coerce_log_level,
        description="stdlib logging level applied to every emit.",
    ),
    "ws_rate_limit_enabled": _AllowlistEntry(
        name="ws_rate_limit_enabled",
        coerce=_coerce_bool,
        description="Per-charger CALL rate limiter (E5-3) kill-switch.",
    ),
    "backend_authorize_cache_enabled": _AllowlistEntry(
        name="backend_authorize_cache_enabled",
        coerce=_coerce_bool,
        description="Per-pod Authorize cache (E3-4) kill-switch.",
    ),
}


class OverrideNotAllowedError(ValueError):
    """Raised when an admin write hits a field that isn't in the
    allowlist. The endpoint translates this into a 400 with a clear
    error envelope so the operator sees what *is* allowed."""


class _RuntimeOverrides:
    """Single in-process store. Constructed once; the public-module
    `_singleton` instance is the one read sites and the admin
    endpoint consult."""

    def __init__(self) -> None:
        self._values: dict[str, Any] = {}
        self._lock = threading.Lock()

    def set(self, name: str, value: Any) -> Any:
        """Validate and store an override. Returns the coerced value
        actually written so callers can echo it back to the operator.

        Raises `OverrideNotAllowedError` if the field isn't allow-
        listed, or `ValueError` if the value fails coercion."""
        entry = _ALLOWLIST.get(name)
        if entry is None:
            raise OverrideNotAllowedError(
                f"{name} is not in the runtime-override allowlist; allowed: {sorted(_ALLOWLIST)}"
            )
        coerced = entry.coerce(value)
        with self._lock:
            self._values[name] = coerced
        log.info("runtime_override.set", field=name, value=coerced)
        return coerced

    def clear(self, name: str) -> bool:
        """Remove an override. Returns True if a value was removed,
        False if there wasn't one. Read sites fall back to Settings
        on the next read."""
        with self._lock:
            existed = name in self._values
            self._values.pop(name, None)
        if existed:
            log.info("runtime_override.clear", field=name)
        return existed

    def get(self, name: str, default: Any) -> Any:
        """Read site shim. Returns the override if set, otherwise
        the caller's default (typically `settings.<name>`)."""
        with self._lock:
            return self._values.get(name, default)

    def all(self) -> dict[str, Any]:
        """Snapshot of all currently-set overrides. The admin GET
        endpoint embeds this in its response. Empty dict means "no
        overrides; everything reads from Settings."""
        with self._lock:
            return dict(self._values)


# Module-level singleton. The admin endpoint and any read site that
# wants override-awareness import this name directly. We don't expose
# the class itself — there's exactly one store per process.
_singleton = _RuntimeOverrides()


def set_override(name: str, value: Any) -> Any:
    """Public setter. Wraps the singleton's `set`."""
    return _singleton.set(name, value)


def clear_override(name: str) -> bool:
    """Public clearer. Wraps the singleton's `clear`."""
    return _singleton.clear(name)


def get_override(name: str, default: Any) -> Any:
    """Public reader. Wraps the singleton's `get`. Used by code
    paths that opt in to runtime overrides."""
    return _singleton.get(name, default)


def all_overrides() -> dict[str, Any]:
    """Snapshot of currently-set overrides. Public for the admin
    GET endpoint."""
    return _singleton.all()


def allowlist() -> dict[str, str]:
    """The allowlist as a `{name: description}` map, for the admin
    endpoint to expose to operators (so a 400 error tells them what
    *is* allowed)."""
    return {entry.name: entry.description for entry in _ALLOWLIST.values()}


__all__ = [
    "OverrideNotAllowedError",
    "all_overrides",
    "allowlist",
    "clear_override",
    "get_override",
    "set_override",
]


# Test seam — let unit tests reset state between cases without
# reaching into private internals.
def _reset_for_tests() -> None:  # pragma: no cover - used by tests only
    """Clear all overrides. Intended for `pytest` fixtures only."""
    with _singleton._lock:
        _singleton._values.clear()


# Type stubs for the literal allowlist names — mypy can spot a typo
# on a `get_override("log_levle", ...)` call site this way.
AllowlistName = Literal[
    "log_level",
    "ws_rate_limit_enabled",
    "backend_authorize_cache_enabled",
]
