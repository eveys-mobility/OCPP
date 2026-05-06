"""Metadata schema enforcement for `Settings`.

ADR-0025 declares every field on `Settings` must carry enough metadata
for the configuration-reference generator: a non-empty `description=`
plus `json_schema_extra` containing `category`, `impact`, `secret`, and
`stability`. This test fails the build whenever a new field lands
without that metadata — the same backstop reviewers would otherwise
have to enforce by hand.
"""

from __future__ import annotations

from eveys_ocpp.settings import Settings

ALLOWED_CATEGORIES = {
    "ws_server",
    "grpc_server",
    "rest_server",
    "kafka_producer",
    "kafka_topics",
    "redis",
    "postgres",
    "identity",
    "logging",
    "ocpp_defaults",
    "cross_pod_bus",
    "idempotency",
    "clickhouse_ingest",
    "backend_integration",
    "authorize_cache",
    "webhooks",
}

ALLOWED_STABILITY = {"tunable", "structural", "dev-only"}

REQUIRED_EXTRA_KEYS = ("category", "impact", "secret", "stability")


def test_every_field_has_a_description() -> None:
    for name, info in Settings.model_fields.items():
        assert info.description, f"{name} is missing a description"


def test_every_field_carries_required_extra_keys() -> None:
    for name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict), f"{name} is missing json_schema_extra"
        for key in REQUIRED_EXTRA_KEYS:
            assert key in extra, f"{name}.json_schema_extra is missing {key!r}"


def test_category_is_in_closed_enum() -> None:
    for name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict)
        category = extra["category"]
        assert category in ALLOWED_CATEGORIES, (
            f"{name}.category={category!r} is not in the closed enum "
            f"defined by ADR-0025: {sorted(ALLOWED_CATEGORIES)}"
        )


def test_stability_is_one_of_three_values() -> None:
    for name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict)
        stability = extra["stability"]
        assert stability in ALLOWED_STABILITY, (
            f"{name}.stability={stability!r} is not one of {sorted(ALLOWED_STABILITY)}"
        )


def test_secret_is_a_bool() -> None:
    for name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict)
        secret = extra["secret"]
        assert isinstance(secret, bool), f"{name}.secret must be bool, got {type(secret)}"


def test_impact_is_non_empty_string() -> None:
    for name, info in Settings.model_fields.items():
        extra = info.json_schema_extra
        assert isinstance(extra, dict)
        impact = extra["impact"]
        assert isinstance(impact, str) and impact.strip(), (
            f"{name}.impact must be a non-empty string"
        )
