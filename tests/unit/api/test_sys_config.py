"""Tests for `GET /api/v1/sys/config` — config introspection."""

from __future__ import annotations

import httpx
import pytest

from eveys_ocpp.api.sys_config import MASK


@pytest.mark.asyncio
async def test_returns_one_entry_per_settings_field(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    assert response.status_code == 200, response.text
    body = response.json()

    assert body["scope"] == "gateway"
    assert "loaded_at" in body
    assert "request_id" in body

    entries = body["entries"]
    keys = {e["key"] for e in entries}
    # Sample of fields that must be present.
    for required in (
        "ws_port",
        "rest_port",
        "kafka_brokers",
        "redis_url",
        "log_level",
        "rest_inbound_tokens",
        "db_url",
        "backend_token",
        "webhook_secret",
    ):
        assert required in keys, f"missing field: {required}"


@pytest.mark.asyncio
async def test_sensitive_fields_are_masked(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()
    entries = {e["key"]: e for e in body["entries"]}

    # The conftest fixture sets `rest_inbound_tokens="test-token-foundation"`;
    # if that string leaks anywhere in the response, masking is broken.
    assert "test-token-foundation" not in response.text

    # Each of these must be marked sensitive AND have a masked / empty value.
    for key in (
        "rest_inbound_tokens",
        "db_url",
        "backend_token",
        "webhook_secret",
        "sentry_dsn",
    ):
        entry = entries[key]
        assert entry["sensitive"] is True, f"{key} not marked sensitive"
        assert entry["value"] in {"", MASK}, f"{key} value not masked: {entry['value']!r}"


@pytest.mark.asyncio
async def test_every_entry_carries_required_metadata(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()

    for entry in body["entries"]:
        # Required keys on every entry.
        for k in (
            "key",
            "value",
            "sensitive",
            "default",
            "source",
            "description",
            "category",
            "stability",
            "mutable",
            "restart",
            "range",
        ):
            assert k in entry, f"{entry['key']} missing field {k}"
        # Description text should never be empty (every Settings field
        # carries one).
        assert entry["description"], f"{entry['key']} has empty description"
        # Restart impact must be one of our enum values.
        assert entry["restart"] in {"none", "gateway", "console", "both"}
        assert entry["source"] in {"env", "default"}


@pytest.mark.asyncio
async def test_allowlisted_fields_are_mutable_with_no_restart(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()
    entries = {e["key"]: e for e in body["entries"]}

    # `log_level` is allowlisted for runtime override (see test_admin.py).
    log_level = entries["log_level"]
    assert log_level["mutable"] is True
    assert log_level["restart"] == "none"

    # `rest_port` is structural — restart=gateway, mutable=False.
    rest_port = entries["rest_port"]
    assert rest_port["mutable"] is False
    assert rest_port["restart"] == "gateway"


@pytest.mark.asyncio
async def test_range_for_bounded_int_includes_constraints(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()
    entries = {e["key"]: e for e in body["entries"]}

    # `ws_port: int = Field(ge=1, le=65535)` — the range string should
    # surface both bounds.
    ws_port = entries["ws_port"]
    assert "1" in ws_port["range"]
    assert "65535" in ws_port["range"]


@pytest.mark.asyncio
async def test_literal_field_lists_allowed_values(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()
    entries = {e["key"]: e for e in body["entries"]}

    # `log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]`
    log_level = entries["log_level"]
    for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        assert level in log_level["range"], f"missing literal {level}"


@pytest.mark.asyncio
async def test_kafka_topic_fields_are_grouped(
    client: httpx.AsyncClient,
) -> None:
    response = await client.get("/api/v1/sys/config")
    body = response.json()
    topics = [e for e in body["entries"] if e["key"].startswith("kafka_topic_")]
    assert len(topics) >= 4
    for entry in topics:
        assert entry["category"] == "kafka_topics"
        assert entry["stability"] == "structural"
