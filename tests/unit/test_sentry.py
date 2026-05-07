"""Sentry integration — init no-op when DSN is empty, PII filter, log capture.

Tests use sentry-sdk's `Transport` mock pattern: drop the real HTTP
transport and capture every envelope the SDK *would* have sent. The
captured envelopes are inspected for content + tag presence; nothing
ever leaves the test process.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
import sentry_sdk

from eveys_ocpp.observability.sentry import (
    _PII_FIELDS,
    _before_send,
    _scrub_value,
    bind_sentry_scope,
    init_sentry,
)
from eveys_ocpp.settings import Settings


class _CapturingTransport(sentry_sdk.transport.Transport):
    """Replaces the real HTTP transport — captures envelopes in memory."""

    def __init__(self) -> None:
        super().__init__()
        self.captured: list[dict[str, Any]] = []

    def capture_envelope(self, envelope: Any) -> None:
        # Walk the envelope items and pull the parsed JSON event payload.
        for item in envelope.items:
            try:
                payload = item.get_event() or item.payload.json
            except Exception:
                continue
            if isinstance(payload, dict):
                self.captured.append(payload)


@pytest.fixture
def reset_sentry_state() -> Iterator[None]:
    """Wipe the module-level `_INITIALISED` flag + clear the SDK hub
    so each test gets a clean slate. Without this, tests after the
    first one would see `init_sentry` short-circuit on the
    re-init guard.
    """
    from eveys_ocpp.observability import sentry as sentry_mod

    sentry_mod._INITIALISED = False
    yield
    # Tear down the test client so other tests don't see leaked state.
    sentry_sdk.get_global_scope().set_client(None)
    sentry_mod._INITIALISED = False


def _settings(**overrides: Any) -> Settings:
    base: dict[str, Any] = {"sentry_dsn": ""}
    base.update(overrides)
    return Settings(**base)


# ---- init_sentry -----------------------------------------------------------


def test_init_sentry_is_noop_when_dsn_is_empty(reset_sentry_state: None) -> None:
    """Empty DSN must not init the SDK — verifies the gateway behaves
    identically to a Sentry-free build (acceptance criterion)."""
    init_sentry(_settings(sentry_dsn=""))
    # `get_client().is_active()` is False when no client has been bound.
    assert not sentry_sdk.get_client().is_active()


def test_init_sentry_idempotent(reset_sentry_state: None) -> None:
    """Second call must short-circuit so test fixtures can be liberal
    and so a re-init in tests doesn't emit the SDK's WARNING."""
    settings = _settings(sentry_dsn="https://key@sentry.example/1")
    init_sentry(settings)
    first_client = sentry_sdk.get_client()
    init_sentry(settings)
    # Same client instance on the second call — proves no re-init.
    assert sentry_sdk.get_client() is first_client


def test_init_sentry_sets_pod_id_tag(reset_sentry_state: None) -> None:
    """`pod_id` should land as a tag on every event from this process."""
    init_sentry(_settings(sentry_dsn="https://key@sentry.example/1", pod_id="pod-7"))
    scope = sentry_sdk.get_isolation_scope()
    assert scope._tags.get("pod_id") == "pod-7"


# ---- before_send / PII filter ---------------------------------------------


def test_before_send_redacts_id_tag_at_top_level() -> None:
    event = {"extra": {"id_tag": "RFID-1234", "ok_field": "keep"}}
    out = _before_send(event, {})
    assert out is not None
    assert out["extra"]["id_tag"] == "[redacted]"
    assert out["extra"]["ok_field"] == "keep"


def test_before_send_redacts_nested_id_tag() -> None:
    event = {
        "contexts": {
            "ocpp": {"id_tag": "RFID-XYZ", "transaction_id": 42},
        },
    }
    out = _before_send(event, {})
    assert out is not None
    assert out["contexts"]["ocpp"]["id_tag"] == "[redacted]"
    assert out["contexts"]["ocpp"]["transaction_id"] == 42


def test_before_send_redacts_in_lists() -> None:
    event = {"breadcrumbs": [{"data": {"id_tag": "x"}}]}
    out = _before_send(event, {})
    assert out is not None
    assert out["breadcrumbs"][0]["data"]["id_tag"] == "[redacted]"


def test_before_send_drops_charger_error_events() -> None:
    """Events explicitly tagged `charger_error=true` must be dropped —
    we don't page operators when the *charger* sends garbage."""
    event = {"tags": {"charger_error": "true"}, "message": "malformed payload"}
    out = _before_send(event, {})
    assert out is None


def test_pii_field_set_covers_id_tag_variants() -> None:
    """Spec requires id_tag scrubbing — keep the field list aligned
    with what handlers actually log."""
    assert "id_tag" in _PII_FIELDS
    assert "id_token" in _PII_FIELDS  # OCPP 2.0.1-style name


def test_scrub_value_passthroughs_primitives() -> None:
    """Strings, ints, None — passed through unchanged. Only dicts /
    lists are walked."""
    assert _scrub_value("hello") == "hello"
    assert _scrub_value(42) == 42
    assert _scrub_value(None) is None


# ---- structlog processor ---------------------------------------------------


def test_bind_sentry_scope_is_noop_when_sentry_off() -> None:
    """When `_INITIALISED` is False the processor must return the
    event_dict untouched and never touch the Sentry SDK."""
    from eveys_ocpp.observability import sentry as sentry_mod

    sentry_mod._INITIALISED = False
    out = bind_sentry_scope(None, "info", {"event": "x", "cp_id": "CP_1"})
    assert out == {"event": "x", "cp_id": "CP_1"}


def test_bind_sentry_scope_sets_tags_when_active(reset_sentry_state: None) -> None:
    """Active SDK + `cp_id`/`request_id` in event_dict → tags appear
    on the current Sentry scope."""
    init_sentry(_settings(sentry_dsn="https://key@sentry.example/1"))
    bind_sentry_scope(
        None,
        "info",
        {"event": "x", "cp_id": "CP_42", "request_id": "req-abc", "rpc": "RemoteStart"},
    )
    scope = sentry_sdk.get_current_scope()
    assert scope._tags.get("cp_id") == "CP_42"
    assert scope._tags.get("request_id") == "req-abc"
    assert scope._tags.get("rpc") == "RemoteStart"


# ---- end-to-end log capture -----------------------------------------------


def test_log_error_produces_sentry_event(reset_sentry_state: None) -> None:
    """End-to-end: configure logging + sentry, call `log.error(...)`,
    verify a Sentry event was captured by our fake transport."""
    transport = _CapturingTransport()
    sentry_sdk.init(
        dsn="https://key@sentry.example/1",
        transport=transport,
        integrations=[
            sentry_sdk.integrations.logging.LoggingIntegration(level=20, event_level=40),
        ],
        before_send=_before_send,
    )
    # Mark our module-level state initialised so bind_sentry_scope acts.
    from eveys_ocpp.observability import sentry as sentry_mod

    sentry_mod._INITIALISED = True

    import logging as stdlib_logging

    logger = stdlib_logging.getLogger("eveys_ocpp.test")
    logger.error("test_error", extra={"id_tag": "RFID-XYZ"})

    sentry_sdk.flush(timeout=2)
    assert transport.captured, "no Sentry event captured"
    # PII filter ran — `id_tag` field should not appear verbatim in
    # any captured event payload.
    serialised = repr(transport.captured)
    assert "RFID-XYZ" not in serialised
