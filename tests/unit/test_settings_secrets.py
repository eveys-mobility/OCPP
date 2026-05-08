"""E5-7 — secret-tagged Settings fields are SecretStr-wrapped, so a
stray `print(settings)` or unstructured-log dump never leaks the
underlying value.

Pairs with the existing `test_settings_metadata.py` schema gate. That
file enforces "every field carries `secret` metadata"; this file
enforces "every field tagged `secret=True` is actually a SecretStr".

Why both: the metadata flag is what the doc generator and `.env.example`
key off, so it has to be correct. The runtime type is what protects
the value at the language level. They have to agree, and a future
contributor adding a sixth secret field needs to add both.
"""

from __future__ import annotations

import json

from pydantic import SecretStr

from eveys_ocpp.settings import Settings

# Live secret values used across the assertions below. Each one is a
# distinctive string so a leak shows up as a clear substring match.
_SENTINEL_SECRETS = {
    "rest_inbound_tokens": "tok-rest-DEADBEEF",
    "db_url": "postgresql://u:pwd-DEADBEEF@host/db",
    "backend_token": "tok-backend-DEADBEEF",
    "webhook_secret": "wh-key-DEADBEEF",
    "sentry_dsn": "https://k-DEADBEEF@sentry.io/123",
}


def _settings_with_sentinels() -> Settings:
    return Settings(**_SENTINEL_SECRETS)  # type: ignore[arg-type]


def test_every_secret_tagged_field_is_a_SecretStr() -> None:
    """A future contributor adding a sixth secret field must remember
    to wrap its type. This test catches the omission."""
    s = Settings()
    for field_name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict)
        if not extra.get("secret"):
            continue
        value = getattr(s, field_name)
        assert isinstance(value, SecretStr), (
            f"Settings.{field_name} is tagged `secret=True` in metadata "
            f"but its runtime value is {type(value).__name__}, not SecretStr. "
            "Wrap the field with `pydantic.SecretStr` (E5-7)."
        )


def test_repr_does_not_leak_any_secret() -> None:
    s = _settings_with_sentinels()
    rendered = repr(s)
    for name, sentinel in _SENTINEL_SECRETS.items():
        assert sentinel not in rendered, (
            f"repr(settings) leaked `{name}`'s value `{sentinel}`. "
            "SecretStr wrapping should redact this — see E5-7."
        )


def test_str_does_not_leak_any_secret() -> None:
    s = _settings_with_sentinels()
    rendered = str(s)
    for name, sentinel in _SENTINEL_SECRETS.items():
        assert sentinel not in rendered, f"str(settings) leaked `{name}`'s value `{sentinel}`."


def test_model_dump_default_does_not_leak() -> None:
    """`model_dump()` is what `structlog`-style loggers call when an
    operator logs the whole settings object. Without `mode='json'` it
    returns SecretStr instances, whose repr is the redacted form."""
    s = _settings_with_sentinels()
    dumped_repr = repr(s.model_dump())
    for name, sentinel in _SENTINEL_SECRETS.items():
        assert sentinel not in dumped_repr, f"model_dump() repr leaked `{name}`'s value."


def test_model_dump_json_redacts_secrets() -> None:
    """JSON-mode dumping (e.g. for a /debug endpoint or a dump-config
    command) must not embed secrets in the output."""
    s = _settings_with_sentinels()
    dumped = s.model_dump_json()
    parsed = json.loads(dumped)
    for name, sentinel in _SENTINEL_SECRETS.items():
        # SecretStr serialises to "**********" under model_dump_json.
        assert parsed[name] != sentinel
        assert sentinel not in dumped


def test_get_secret_value_returns_original() -> None:
    """The wrapping is one-way for printing only; `.get_secret_value()`
    is the explicit retrieval API every call site uses."""
    s = _settings_with_sentinels()
    for name, sentinel in _SENTINEL_SECRETS.items():
        wrapped = getattr(s, name)
        assert wrapped.get_secret_value() == sentinel


def test_str_input_is_coerced_to_SecretStr() -> None:
    """Existing test code that passes plain `str` for a SecretStr
    field still works — Pydantic auto-coerces. This guards against a
    SecretStr field being introduced with `validate_default=False` or
    similar that would break the coercion."""
    s = Settings(rest_inbound_tokens="abc,def")
    assert isinstance(s.rest_inbound_tokens, SecretStr)
    assert s.rest_inbound_tokens.get_secret_value() == "abc,def"
