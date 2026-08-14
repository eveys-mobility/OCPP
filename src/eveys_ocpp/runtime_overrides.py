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


def _coerce_str(value: Any) -> str:
    """Plain string coercion for free-form values.

    Rejects the container types a JSON body can produce so a caller
    that sends `["A","B"]` for a comma-separated field gets a clear
    error rather than the stringified list `"['A', 'B']"` silently
    becoming the value.
    """
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float, bool)) or value is None:
        raise ValueError(f"expected string, got {value!r}")
    raise ValueError(f"expected string, got {type(value).__name__}")


def _coerce_url_or_empty(value: Any) -> str:
    """A URL field that may be empty (the empty string means "fall
    back to the global default" in the dispatcher). Validates the
    HTTP/HTTPS scheme but tolerates the empty value."""
    if not isinstance(value, str):
        raise ValueError(f"expected string URL, got {value!r}")
    stripped = value.strip()
    if stripped == "":
        return ""
    if not (stripped.startswith("http://") or stripped.startswith("https://")):
        raise ValueError(
            f"expected http(s):// URL, got {value!r}; "
            "use the empty string to fall back to the global default"
        )
    return stripped


def _coerce_int_in_range(low: int, high: int, field: str) -> Callable[[Any], int]:
    """Build a coercer that accepts JSON numbers / strings and clamps
    to the [low, high] inclusive range. Used by the OCPP post-boot
    config push allowlist where each numeric key has its own bounds
    (mirrors the Settings field's ``ge`` / ``le``)."""

    def coerce(value: Any) -> int:
        if isinstance(value, bool):
            # `bool is int` in Python; reject the int subclass so a
            # JSON `true` doesn't silently become `1`.
            raise ValueError(f"{field}: expected integer, got bool {value!r}")
        if isinstance(value, int):
            n = value
        elif isinstance(value, str):
            try:
                n = int(value.strip())
            except ValueError as exc:
                raise ValueError(f"{field}: expected integer, got {value!r}") from exc
        else:
            raise ValueError(f"{field}: expected integer, got {value!r}")
        if n < low or n > high:
            raise ValueError(f"{field}: {n} outside [{low}, {high}]")
        return n

    return coerce


def _coerce_url(value: Any) -> str:
    """Like `_coerce_url_or_empty` but rejects the empty value. Used
    for the global `webhook_base_url` since clearing it disables the
    entire dispatcher and that's better done by setting an empty
    env var at restart, not by an inline edit."""
    if not isinstance(value, str):
        raise ValueError(f"expected string URL, got {value!r}")
    stripped = value.strip()
    if stripped == "":
        raise ValueError(
            "webhook_base_url cannot be cleared via runtime override "
            "(would disable the dispatcher); set the env var to '' and restart"
        )
    if not (stripped.startswith("http://") or stripped.startswith("https://")):
        raise ValueError(f"expected http(s):// URL, got {value!r}")
    return stripped


# Per-event webhook enable flags share an asymmetry that operators
# need to know about, so we share the explanation rather than re-typing
# it on every entry.
_WEBHOOK_ENABLE_ASYMMETRY = (
    " Asymmetry: turning OFF is fully live (delivery is suppressed on "
    "the next event); turning ON is live ONLY if this event was already "
    "enabled at boot — _enabled_topics() runs once at consumer start, so "
    "a topic that wasn't subscribed then needs a pod restart to begin "
    "delivering."
)


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
    "webhook_base_url": _AllowlistEntry(
        name="webhook_base_url",
        coerce=_coerce_url,
        description=(
            "Default base URL for webhook delivery. Per-event URLs that "
            "are empty fall back to `<base>/<event-slug>`. Cannot be "
            "cleared via runtime override — clearing it disables the "
            "dispatcher entirely, which is a deploy-time call."
        ),
    ),
    "webhook_url_cp_boot": _AllowlistEntry(
        name="webhook_url_cp_boot",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for cp.boot webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/cp-boot`."
        ),
    ),
    "webhook_url_cp_online": _AllowlistEntry(
        name="webhook_url_cp_online",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for cp.online webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/cp-online`."
        ),
    ),
    "webhook_url_cp_offline": _AllowlistEntry(
        name="webhook_url_cp_offline",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for cp.offline webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/cp-offline`."
        ),
    ),
    "webhook_url_cp_status": _AllowlistEntry(
        name="webhook_url_cp_status",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for cp.status_changed webhook deliveries. Empty "
            "string falls back to `<webhook_base_url>/cp-status-changed`."
        ),
    ),
    "webhook_url_cp_meter": _AllowlistEntry(
        name="webhook_url_cp_meter",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for cp.meter webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/cp-meter`."
        ),
    ),
    "webhook_url_tx_started": _AllowlistEntry(
        name="webhook_url_tx_started",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for tx.started webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/tx-started`."
        ),
    ),
    "webhook_url_tx_stopped": _AllowlistEntry(
        name="webhook_url_tx_stopped",
        coerce=_coerce_url_or_empty,
        description=(
            "Override URL for tx.stopped webhook deliveries. Empty string "
            "falls back to `<webhook_base_url>/tx-stopped`."
        ),
    ),
    "webhook_enable_cp_boot": _AllowlistEntry(
        name="webhook_enable_cp_boot",
        coerce=_coerce_bool,
        description="cp.boot webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_cp_online": _AllowlistEntry(
        name="webhook_enable_cp_online",
        coerce=_coerce_bool,
        description="cp.online webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_cp_offline": _AllowlistEntry(
        name="webhook_enable_cp_offline",
        coerce=_coerce_bool,
        description="cp.offline webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_cp_status": _AllowlistEntry(
        name="webhook_enable_cp_status",
        coerce=_coerce_bool,
        description="cp.status_changed webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_cp_meter": _AllowlistEntry(
        name="webhook_enable_cp_meter",
        coerce=_coerce_bool,
        description="cp.meter webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_tx_started": _AllowlistEntry(
        name="webhook_enable_tx_started",
        coerce=_coerce_bool,
        description="tx.started webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    "webhook_enable_tx_stopped": _AllowlistEntry(
        name="webhook_enable_tx_stopped",
        coerce=_coerce_bool,
        description="tx.stopped webhook delivery toggle." + _WEBHOOK_ENABLE_ASYMMETRY,
    ),
    # ---- OCPP boot configs ----
    # Each key here is read fresh by `handlers.v16.boot_notification.
    # _post_boot_keys` on every boot, so an operator edit takes effect
    # on the next boot without restarting the gateway. AC/DC measurand
    # variants are independently settable so a mixed-site operator
    # can tune both without restarting.
    "meter_value_sample_interval_seconds": _AllowlistEntry(
        name="meter_value_sample_interval_seconds",
        coerce=_coerce_int_in_range(5, 3600, "meter_value_sample_interval_seconds"),
        description="Seconds between MeterValues samples during a transaction.",
    ),
    "ocpp_cfg_heartbeat_interval_seconds": _AllowlistEntry(
        name="ocpp_cfg_heartbeat_interval_seconds",
        coerce=_coerce_int_in_range(10, 86400, "ocpp_cfg_heartbeat_interval_seconds"),
        description="HeartbeatInterval pushed via ChangeConfiguration after boot.",
    ),
    "ocpp_cfg_connection_time_out_seconds": _AllowlistEntry(
        name="ocpp_cfg_connection_time_out_seconds",
        coerce=_coerce_int_in_range(1, 600, "ocpp_cfg_connection_time_out_seconds"),
        description="ConnectionTimeOut (seconds) pushed after boot.",
    ),
    "ocpp_cfg_transaction_message_attempts": _AllowlistEntry(
        name="ocpp_cfg_transaction_message_attempts",
        coerce=_coerce_int_in_range(1, 20, "ocpp_cfg_transaction_message_attempts"),
        description="TransactionMessageAttempts pushed after boot.",
    ),
    "ocpp_cfg_transaction_message_retry_interval_seconds": _AllowlistEntry(
        name="ocpp_cfg_transaction_message_retry_interval_seconds",
        coerce=_coerce_int_in_range(1, 3600, "ocpp_cfg_transaction_message_retry_interval_seconds"),
        description="TransactionMessageRetryInterval (seconds) pushed after boot.",
    ),
    "ocpp_cfg_websocket_ping_interval_seconds": _AllowlistEntry(
        name="ocpp_cfg_websocket_ping_interval_seconds",
        coerce=_coerce_int_in_range(5, 3600, "ocpp_cfg_websocket_ping_interval_seconds"),
        description="WebSocketPingInterval (seconds) pushed after boot.",
    ),
    "ocpp_cfg_post_boot_push_enabled": _AllowlistEntry(
        name="ocpp_cfg_post_boot_push_enabled",
        coerce=_coerce_bool,
        description=(
            "Master switch for the post-boot ChangeConfiguration push. "
            "Off means the gateway pushes nothing after BootNotification."
        ),
    ),
    "ocpp_cfg_post_boot_push_skip_keys": _AllowlistEntry(
        name="ocpp_cfg_post_boot_push_skip_keys",
        coerce=_coerce_str,
        description=(
            "Comma-separated OCPP keys to omit from the post-boot push "
            "(case-insensitive); empty pushes every key."
        ),
    ),
    "ocpp_cfg_iso15118_pnc_enabled": _AllowlistEntry(
        name="ocpp_cfg_iso15118_pnc_enabled",
        coerce=_coerce_bool,
        description=(
            "ISO15118PnCEnabled pushed after boot (ISO 15118 Plug-and-Charge master switch)."
        ),
    ),
    "ocpp_cfg_plug_and_charge_mode": _AllowlistEntry(
        name="ocpp_cfg_plug_and_charge_mode",
        coerce=_coerce_int_in_range(0, 2, "ocpp_cfg_plug_and_charge_mode"),
        description=(
            "PlugandChargeMode pushed after boot: 0=EIM only, 1=EIM preferred, 2=PnC preferred."
        ),
    ),
    "ocpp_cfg_contract_validation_offline": _AllowlistEntry(
        name="ocpp_cfg_contract_validation_offline",
        coerce=_coerce_bool,
        description=(
            "ContractValidationOffline pushed after boot "
            "(trust cached ISO 15118 contract during backend outage)."
        ),
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
    "webhook_base_url",
    "webhook_url_cp_boot",
    "webhook_url_cp_online",
    "webhook_url_cp_offline",
    "webhook_url_cp_status",
    "webhook_url_cp_meter",
    "webhook_url_tx_started",
    "webhook_url_tx_stopped",
    "webhook_enable_cp_boot",
    "webhook_enable_cp_online",
    "webhook_enable_cp_offline",
    "webhook_enable_cp_status",
    "webhook_enable_cp_meter",
    "webhook_enable_tx_started",
    "webhook_enable_tx_stopped",
    "meter_value_sample_interval_seconds",
    "ocpp_cfg_heartbeat_interval_seconds",
    "ocpp_cfg_connection_time_out_seconds",
    "ocpp_cfg_transaction_message_attempts",
    "ocpp_cfg_transaction_message_retry_interval_seconds",
    "ocpp_cfg_websocket_ping_interval_seconds",
    "ocpp_cfg_post_boot_push_enabled",
    "ocpp_cfg_post_boot_push_skip_keys",
    "ocpp_cfg_iso15118_pnc_enabled",
    "ocpp_cfg_plug_and_charge_mode",
    "ocpp_cfg_contract_validation_offline",
]
