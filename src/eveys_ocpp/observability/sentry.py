"""Sentry error tracking — opt-in, PII-stripped, OTel-friendly.

Contract for the rest of the codebase: nothing. The SDK init wires
into stdlib `logging` so existing `log.exception(...)` /
`log.error(...)` calls surface as Sentry events with no caller
plumbing. Per-request context (`cp_id`, `pod_id`, `request_id`) is
bound to the Sentry scope from a structlog processor — the same
contextvars that drive log fields.

Boot order in `__main__.py`:

    init_sentry(settings)        # first — catches its own init errors
    configure_logging(...)       # second — installs the sentry log processor
    configure_tracing(settings)  # third — OTel owns spans

Empty DSN (`EVEYS_OCPP_SENTRY_DSN=""`) is a hard no-op: no init, no
transport, no monkey-patches. The gateway behaves identically to a
build without sentry-sdk on the path.

Sampling defaults:

- `traces_sample_rate=0.0` — OTel (E4-3) owns tracing. Setting > 0
  doubles instrumentation overhead and fragments traces across two
  backends. Operators who specifically want Sentry's `Performance`
  view can opt in.
- `profiles_sample_rate=0.0` — profiling needs a positive
  `traces_sample_rate` to attach to anything anyway.

PII filter: `id_tag` is the EV driver's RFID — personally identifying.
The `before_send` hook walks events and redacts any `id_tag` /
`id_token` field by name. Stack traces, breadcrumbs, and
extra-context dicts all go through the same scrubber.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from eveys_ocpp import __version__

if TYPE_CHECKING:
    from structlog.types import EventDict

    from eveys_ocpp.settings import Settings

# Set to True the first time `init_sentry` actually inits the SDK.
# Re-entrant calls become no-ops — `sentry_sdk.init` is itself
# idempotent but emits a noisy WARNING on every re-call.
_INITIALISED = False

# Field names whose values we redact in `before_send`. Keep this list
# small and exact — broader scrubbing risks losing useful debugging
# context. Lower-cased for case-insensitive comparison.
_PII_FIELDS: frozenset[str] = frozenset(
    {
        "id_tag",
        "id_token",
        "parent_id_tag",
        "parent_id_token",
    }
)

_REDACTED = "[redacted]"


def _scrub_value(value: Any) -> Any:
    """Recursively redact PII fields in any nested dict / list.

    Called from `before_send` on every event dict and breadcrumb. The
    cost is per-event and only fires when an event is actually being
    sent, not on every log call.
    """
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            out[k] = _REDACTED if k.lower() in _PII_FIELDS else _scrub_value(v)
        return out
    if isinstance(value, list):
        return [_scrub_value(v) for v in value]
    return value


def _before_send(event: Any, _hint: Any) -> Any:
    """Sentry `before_send` hook — strips PII and drops noisy classes.

    Returning `None` drops the event entirely; that's how we silence
    e.g. `OCPPChargerError`-tagged logs that the spec explicitly says
    should not page operators (charger sent garbage, not our bug).

    Typed as `Any` because Sentry's `Event` is a deeply-nested
    TypedDict that isn't worth importing here just to satisfy the
    callback signature; we treat the event as an opaque dict at the
    boundary.
    """
    # Drop events the gateway has explicitly tagged as charger-side.
    tags = event.get("tags") or {}
    if tags.get("charger_error") == "true":
        return None

    # PII scrub: walk the whole event dict.
    return _scrub_value(event)


def init_sentry(settings: Settings) -> None:
    """Initialise the Sentry SDK iff `settings.sentry_dsn` is set.

    Idempotent — second calls are no-ops so test fixtures can be
    liberal. Imports `sentry_sdk` lazily so a Sentry-disabled boot
    never pulls the SDK's dependencies into the import graph.
    """
    global _INITIALISED
    if _INITIALISED:
        return
    # E5-7: sentry_dsn is a SecretStr; unwrap once and reuse below.
    # An empty string disables the SDK entirely; any non-empty DSN
    # boots it. `bool(SecretStr(""))` is True (any object is truthy),
    # so the explicit get_secret_value() is required for the gate.
    dsn_value = settings.sentry_dsn.get_secret_value()
    if not dsn_value:
        return

    # Lazy import — keeps `sentry_sdk` and its deps off the hot
    # import path for Sentry-disabled runs.
    import sentry_sdk
    from sentry_sdk.integrations.logging import LoggingIntegration

    # Capture log records at ERROR+ as Sentry events, INFO+ as
    # breadcrumbs. Matches Sentry's default; spelled out so the
    # threshold is greppable.
    logging_integration = LoggingIntegration(
        level=20,  # logging.INFO — breadcrumb threshold
        event_level=40,  # logging.ERROR — Sentry-event threshold
    )

    release = settings.sentry_release or __version__

    sentry_sdk.init(
        dsn=dsn_value,
        environment=settings.sentry_environment,
        release=release,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        profiles_sample_rate=settings.sentry_profiles_sample_rate,
        integrations=[logging_integration],
        before_send=_before_send,
        # `send_default_pii=False` keeps the SDK from auto-collecting
        # IPs, cookies, request bodies, etc. We only want what we
        # explicitly attach via the structlog processor.
        send_default_pii=False,
        # `attach_stacktrace=False` — stack traces only travel with
        # actual exceptions, not every log line. Cuts payload size.
        attach_stacktrace=False,
    )

    # Pin pod_id as a tag on every event sent from this process —
    # operators routinely filter Sentry by pod when triaging.
    sentry_sdk.set_tag("pod_id", settings.pod_id)

    _INITIALISED = True


def bind_sentry_scope(_logger: Any, _name: str, event_dict: EventDict) -> EventDict:
    """structlog processor — mirrors per-request context onto Sentry.

    Runs on every log call (cheap when Sentry is off — the
    `sentry_sdk.get_current_scope()` call returns a no-op scope). Pulls
    `cp_id`, `request_id`, `rpc`, `action` (the four contextvars the
    gateway already binds via structlog) and sets them as Sentry tags
    so a Sentry event arrives with the same dimensions log lines
    carry.

    Why a structlog processor and not `bind_contextvars`-side? Because
    contextvars are bound at request entry from many places (WS
    handler dispatch, REST middleware, gRPC dispatcher) — wiring
    Sentry into each would be repetitive. The log-side processor sees
    everything in one place.
    """
    # Lazy import — when Sentry isn't active the SDK isn't loaded.
    try:
        import sentry_sdk
    except ImportError:
        return event_dict

    if not _INITIALISED:
        return event_dict

    scope = sentry_sdk.get_current_scope()
    for key in ("cp_id", "request_id", "rpc", "action", "pod_id"):
        value = event_dict.get(key)
        if value is not None:
            scope.set_tag(key, str(value))
    return event_dict


__all__ = ["bind_sentry_scope", "init_sentry"]
