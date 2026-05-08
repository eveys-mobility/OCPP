"""Tests for the admin runtime-config endpoints (GET / PATCH / DELETE
under `/api/v1/admin/config`)."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from eveys_ocpp.runtime_overrides import _reset_for_tests


@pytest.fixture(autouse=True)
def reset_overrides() -> Iterator[None]:
    """Each test starts and ends with no overrides set so they don't
    bleed across the suite. The override store is a module-level
    singleton; tests need this isolation explicitly."""
    _reset_for_tests()
    yield
    _reset_for_tests()


@pytest.mark.asyncio
async def test_get_returns_settings_with_secrets_redacted(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/admin/config")

    assert response.status_code == 200, response.text
    body = response.json()
    assert "settings" in body
    assert "overrides" in body
    assert "allowlist" in body
    assert body["scope"] == "per-pod"

    # SecretStr fields auto-redact in model_dump(mode="json").
    # The conftest fixture sets `rest_inbound_tokens="test-token-foundation"`;
    # if that value leaks the redaction is broken.
    assert "test-token-foundation" not in response.text
    # The redacted form is what SecretStr emits.
    assert body["settings"]["rest_inbound_tokens"] == "**********"

    # No overrides initially.
    assert body["overrides"] == {}

    # Allowlist exposes what PATCH will accept — the operator UI / CLI
    # uses this to render the right form.
    assert "log_level" in body["allowlist"]
    assert "ws_rate_limit_enabled" in body["allowlist"]
    assert "backend_authorize_cache_enabled" in body["allowlist"]


@pytest.mark.asyncio
async def test_patch_applies_allowlisted_field(
    client: httpx.AsyncClient,
) -> None:
    response = await client.patch(
        "/api/v1/admin/config",
        json={"updates": {"log_level": "DEBUG"}},
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body["applied"] == {"log_level": "DEBUG"}
    assert body["overrides"] == {"log_level": "DEBUG"}

    # GET reflects the override.
    follow_up = (await client.get("/api/v1/admin/config")).json()
    assert follow_up["overrides"] == {"log_level": "DEBUG"}


@pytest.mark.asyncio
async def test_patch_applies_bool_with_tolerant_coercion(
    client: httpx.AsyncClient,
) -> None:
    """Operators send `true` / `"true"` / `1` interchangeably; the
    coerce layer absorbs the difference so the runtime sees a real
    bool either way."""
    response = await client.patch(
        "/api/v1/admin/config",
        json={"updates": {"ws_rate_limit_enabled": "false"}},
    )

    assert response.status_code == 200, response.text
    assert response.json()["applied"] == {"ws_rate_limit_enabled": False}


@pytest.mark.asyncio
async def test_patch_rejects_non_allowlisted_field(
    client: httpx.AsyncClient,
) -> None:
    response = await client.patch(
        "/api/v1/admin/config",
        json={"updates": {"db_url": "postgresql://attacker/..."}},
    )

    assert response.status_code == 400, response.text
    body = response.json()
    assert body["error_code"] == "BAD_REQUEST"
    # The error message tells the operator what *is* allowed.
    assert "log_level" in body["error"]


@pytest.mark.asyncio
async def test_patch_rejects_invalid_log_level(
    client: httpx.AsyncClient,
) -> None:
    """Coercion rejects values outside the Literal."""
    response = await client.patch(
        "/api/v1/admin/config",
        json={"updates": {"log_level": "VERBOSE"}},
    )

    assert response.status_code == 400, response.text
    assert "VERBOSE" in response.json()["error"]


@pytest.mark.asyncio
async def test_patch_with_mixed_allowed_and_rejected_aborts_with_partial(
    client: httpx.AsyncClient,
) -> None:
    """Atomicity isn't promised — applied fields stay applied even if
    a sibling fails. The error message tells the operator both what
    landed and what didn't, so they can revert deliberately."""
    response = await client.patch(
        "/api/v1/admin/config",
        json={
            "updates": {
                "log_level": "DEBUG",  # allowed
                "db_url": "postgresql://attacker",  # rejected
            }
        },
    )

    assert response.status_code == 400
    body = response.json()
    # The applied field stays in effect — visible via GET.
    follow_up = (await client.get("/api/v1/admin/config")).json()
    assert follow_up["overrides"] == {"log_level": "DEBUG"}
    # The error message names both halves.
    assert "applied" in body["error"]
    assert "rejected" in body["error"]


@pytest.mark.asyncio
async def test_patch_with_empty_updates_400s(
    client: httpx.AsyncClient,
) -> None:
    response = await client.patch("/api/v1/admin/config", json={"updates": {}})

    assert response.status_code == 400


@pytest.mark.asyncio
async def test_delete_clears_a_single_override(
    client: httpx.AsyncClient,
) -> None:
    # Set then clear.
    set_response = await client.patch(
        "/api/v1/admin/config",
        json={"updates": {"log_level": "DEBUG"}},
    )
    assert set_response.status_code == 200

    del_response = await client.delete("/api/v1/admin/config/overrides/log_level")
    assert del_response.status_code == 200, del_response.text
    body = del_response.json()
    assert body["cleared"] is True
    assert body["overrides"] == {}

    # GET confirms.
    follow_up = (await client.get("/api/v1/admin/config")).json()
    assert follow_up["overrides"] == {}


@pytest.mark.asyncio
async def test_delete_on_unset_key_is_idempotent(
    client: httpx.AsyncClient,
) -> None:
    """Clearing a key that was never set returns `cleared: false`
    rather than 404 — DELETE is idempotent at this layer."""
    response = await client.delete("/api/v1/admin/config/overrides/log_level")

    assert response.status_code == 200
    assert response.json()["cleared"] is False
