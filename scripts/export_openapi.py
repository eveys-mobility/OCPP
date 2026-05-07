"""Render the gateway's OpenAPI document to `docs/api/openapi.{json,yaml}`.

Run via `make openapi-export`. The committed files are the canonical
artifact for sharing with backend teams / importing into Postman /
hosting an external Swagger UI; the `EVEYS_OCPP_REST_OPENAPI_ENABLED`
runtime toggle is the dev-time clickable equivalent.

The script ignores the project's `.env` so a developer's local
overrides don't leak into the committed spec — tests and CI both
need to produce a deterministic file.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import yaml

# Imported via the same path the runtime uses; nothing here depends on
# Settings beyond the `rest_openapi_enabled=True` override we pass in.
from eveys_ocpp.api._app import make_app
from eveys_ocpp.settings import Settings

REPO_ROOT = Path(__file__).resolve().parent.parent
JSON_PATH = REPO_ROOT / "docs" / "api" / "openapi.json"
YAML_PATH = REPO_ROOT / "docs" / "api" / "openapi.yaml"


def _build_spec() -> dict[str, object]:
    settings = Settings(
        _env_file=None,
        rest_openapi_enabled=True,
        # A non-empty token allowlist + a sentinel `pod_id` so the
        # generated spec captures the production-shaped Settings, not
        # the dev defaults.
        rest_inbound_tokens="dev-token",
        pod_id="export-script",
    )
    # `make_app` accepts None for every state slot; we don't call any
    # routes, just collect the schema.
    app = make_app(
        session_factory=None,  # type: ignore[arg-type]
        settings=settings,
        registry=None,
        redis=None,  # type: ignore[arg-type]
        command_service=None,
        ch_client=None,
    )
    return dict(app.openapi())


def render(*, check: bool = False) -> int:
    spec = _build_spec()

    json_blob = json.dumps(spec, indent=2, sort_keys=True) + "\n"
    yaml_blob = yaml.safe_dump(spec, sort_keys=True, allow_unicode=True)

    if check:
        # `--check` mode for CI: exit non-zero if the committed files
        # don't match the freshly-rendered output. The diff is
        # printed to stderr so CI logs surface the change.
        existing_json = JSON_PATH.read_text() if JSON_PATH.exists() else ""
        existing_yaml = YAML_PATH.read_text() if YAML_PATH.exists() else ""
        drift = []
        if existing_json != json_blob:
            drift.append(str(JSON_PATH))
        if existing_yaml != yaml_blob:
            drift.append(str(YAML_PATH))
        if drift:
            print(
                "openapi spec drift detected; run `make openapi-export`:\n  " + "\n  ".join(drift),
                file=sys.stderr,
            )
            return 1
        return 0

    JSON_PATH.parent.mkdir(parents=True, exist_ok=True)
    JSON_PATH.write_text(json_blob)
    YAML_PATH.write_text(yaml_blob)
    print(f"wrote {JSON_PATH}")
    print(f"wrote {YAML_PATH}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="Exit non-zero if the committed files don't match the rendered spec.",
    )
    args = parser.parse_args()
    return render(check=args.check)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
