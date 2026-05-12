"""OpenAPI surface — toggle behaviour + schema sanity check.

Three things we lock down here:

1. With `rest_openapi_enabled=False` (the default), `/api/v1/docs`,
   `/api/v1/redoc`, and `/api/v1/openapi.json` all 404. The
   gateway must not self-publish a discoverable schema in
   production.

2. With `rest_openapi_enabled=True`, the three paths exist and
   `/api/v1/openapi.json` returns a valid OpenAPI 3.x document.

3. The committed `docs/api/openapi.{json,yaml}` files are not
   stale relative to the FastAPI app (mirrors the
   `make openapi-export-check` CI gate).

We don't assert on specific schema or example contents — that's
fragile and the static-export drift check covers it cheaply via
plain-text diff.
"""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]


def _make_app(rest_openapi_enabled: bool):  # type: ignore[no-untyped-def]
    settings = Settings(
        _env_file=None,
        rest_openapi_enabled=rest_openapi_enabled,
        rest_inbound_tokens="",
        rest_auth_disabled=True,  # so the test can hit the routes directly
        # Match the openapi exporter — the SSE route is part of the
        # contract the Console codes against, so the snapshot test
        # must build with the same flag.
        sse_enabled=True,
    )
    return make_app(
        session_factory=None,  # type: ignore[arg-type]
        settings=settings,
        registry=None,
        redis=None,  # type: ignore[arg-type]
        command_service=None,
        ch_client=None,
    )


def test_openapi_disabled_by_default_returns_404() -> None:
    app = _make_app(rest_openapi_enabled=False)
    client = TestClient(app)
    for path in ("/api/v1/openapi.json", "/api/v1/docs", "/api/v1/redoc"):
        response = client.get(path)
        assert response.status_code == 404, (
            f"{path} should 404 when rest_openapi_enabled=False; got {response.status_code}"
        )


def test_openapi_enabled_serves_a_valid_spec() -> None:
    app = _make_app(rest_openapi_enabled=True)
    client = TestClient(app)
    response = client.get("/api/v1/openapi.json")
    assert response.status_code == 200, response.text
    spec = response.json()
    # Standard OpenAPI envelope shape — `openapi`, `info`, `paths`.
    assert spec.get("openapi", "").startswith("3."), (
        f"unexpected openapi version: {spec.get('openapi')!r}"
    )
    assert spec.get("info", {}).get("title") == "eveys/ocpp gateway"
    assert isinstance(spec.get("paths"), dict) and spec["paths"], "paths should not be empty"


def test_swagger_ui_loads_when_enabled() -> None:
    app = _make_app(rest_openapi_enabled=True)
    client = TestClient(app)
    response = client.get("/api/v1/docs")
    assert response.status_code == 200
    # Swagger UI's HTML envelope; we just check the title shows up.
    assert "swagger" in response.text.lower()


def test_redoc_loads_when_enabled() -> None:
    app = _make_app(rest_openapi_enabled=True)
    client = TestClient(app)
    response = client.get("/api/v1/redoc")
    assert response.status_code == 200
    assert "redoc" in response.text.lower()


def test_committed_openapi_files_match_the_app() -> None:
    """The committed `docs/api/openapi.{json,yaml}` must match what
    the FastAPI app emits today. CI runs `make openapi-export-check`
    for the same purpose; this unit test runs in `make tests` so a
    contributor catches drift before pushing.

    We compare on the JSON form (parsed) so YAML key-ordering
    differences don't fight us.
    """
    expected = json.loads((REPO_ROOT / "docs" / "api" / "openapi.json").read_text())
    app = _make_app(rest_openapi_enabled=True)
    actual = app.openapi()
    # FastAPI mutates `app.openapi_schema` after first call; the
    # second call returns the cached dict, but the dict may be the
    # exact same instance the test reads. Convert through JSON to
    # normalise.
    actual_normalised = json.loads(json.dumps(actual))
    assert actual_normalised == expected, (
        "committed docs/api/openapi.json drifts from the FastAPI app; "
        "run `make openapi-export` and commit the result"
    )


def test_named_schemas_carry_examples() -> None:
    """Every Pydantic model in `_schemas.py` declares an `example` via
    `model_config["json_schema_extra"]`. Spot-check that the response
    schemas show up with examples in the generated spec — this is
    what populates Swagger UI's `Try it out` defaults."""
    app = _make_app(rest_openapi_enabled=True)
    spec = app.openapi()
    schemas = spec.get("components", {}).get("schemas", {})
    expected_with_examples = {
        "ChargePointDetail",
        "ChargePointListResponse",
        "ErrorEnvelope",
        "HealthResponse",
        "TransactionDetail",
        "TransactionListResponse",
        "RemoteStartRequest",
        "RemoteStopRequest",
        "ResetRequest",
    }
    missing = expected_with_examples - set(schemas.keys())
    assert not missing, f"missing schemas: {sorted(missing)}"
    schemas_without_example = [
        name for name in expected_with_examples if "example" not in schemas[name]
    ]
    assert not schemas_without_example, f"schemas missing `example`: {schemas_without_example}"
