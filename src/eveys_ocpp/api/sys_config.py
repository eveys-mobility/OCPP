"""`GET /api/v1/sys/config` — read-only configuration introspection.

Returns every gateway settings field with the metadata an SRE wants in
front of them: description, accepted range (derived from pydantic
constraints), default, source (env-set vs schema default), restart
impact, mutability (whether the runtime-override allowlist accepts a
PATCH), category and impact text, plus the current value (sensitive
fields masked at the gateway).

Distinct from `/api/v1/admin/config`:
- `/admin/config` returns the model dump + the override allowlist; it
  is the *write surface* (PATCH for allowlisted fields).
- `/sys/config` returns the same data plus per-field metadata that the
  operator UI renders. Read-only.

Auth follows the existing `/api/v1/*` bearer-token middleware.
"""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, Request
from pydantic import SecretStr
from pydantic.fields import FieldInfo

from eveys_ocpp.observability import get_logger
from eveys_ocpp.runtime_overrides import allowlist
from eveys_ocpp.settings import Settings

log = get_logger(__name__)

router = APIRouter(tags=["sys-config"])

MASK = "••••••••"
ENV_PREFIX = "EVEYS_OCPP_"

# Set once at module-import time; the operator UI surfaces this so a
# fresh restart is visible. Settings load happens at app boot, but for
# this endpoint we just need an "approximately when did the process
# start" anchor — process import time is close enough.
_LOADED_AT = datetime.now(UTC).isoformat()


def _stringify(value: Any) -> str:
    """Stringify a settings value for transport. SecretStr renders to
    its redacted placeholder (handled by Pydantic), so by the time we
    see one here the secret is already gone."""
    if value is None:
        return ""
    if isinstance(value, SecretStr):
        secret = value.get_secret_value()
        return MASK if secret else ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list | tuple):
        return ",".join(str(v) for v in value)
    return str(value)


def _is_sensitive(field_info: FieldInfo) -> bool:
    """A field is sensitive if either the type is SecretStr or the
    field carries `secret: True` in `json_schema_extra`."""
    extra = field_info.json_schema_extra or {}
    if isinstance(extra, dict) and extra.get("secret") is True:
        return True
    annotation = field_info.annotation
    return annotation is SecretStr


def _range_text(field_info: FieldInfo) -> str:
    """Derive a human-readable range string from the pydantic
    constraints (`ge`/`le`/`gt`/`lt`) and the type annotation. Falls
    back to the type's name for free-form fields."""
    parts: list[str] = []
    metadata = field_info.metadata or []
    for entry in metadata:
        cls_name = type(entry).__name__
        # `Ge(ge=...)` etc. are dataclass-shaped; pull the numeric
        # bound off the matching attribute.
        for attr in ("ge", "gt", "le", "lt"):
            if hasattr(entry, attr):
                bound = getattr(entry, attr)
                if bound is not None:
                    sym = {"ge": "≥", "gt": ">", "le": "≤", "lt": "<"}[attr]
                    parts.append(f"{sym} {bound}")
                    break
        else:
            # Some constraint types we don't introspect — skip with a hint.
            if cls_name not in {"_PydanticGeneralMetadata"}:
                continue

    annotation = field_info.annotation
    type_hint = ""
    # `Literal[...]` carries its allowed values on `__args__`.
    if (
        annotation is not None
        and hasattr(annotation, "__args__")
        and getattr(annotation, "__origin__", None) is not None
        and str(getattr(annotation, "__origin__", "")).endswith("Literal")
    ):
        return " | ".join(repr(a) for a in annotation.__args__)

    if annotation is bool:
        type_hint = "true | false"
    elif annotation is int:
        type_hint = "integer"
    elif annotation is float:
        type_hint = "float"
    elif annotation is str or annotation is SecretStr:
        type_hint = "string"

    if parts and type_hint:
        return f"{type_hint} ({' '.join(parts)})"
    if parts:
        return " ".join(parts)
    return type_hint or "free-form"


def _restart_impact(field_name: str, allowlisted: set[str]) -> str:
    """Live-mutable allowlisted fields → `none`; everything else needs
    a gateway restart to apply."""
    return "none" if field_name in allowlisted else "gateway"


def _source(field_name: str) -> str:
    """Env if the corresponding `EVEYS_OCPP_<FIELD>` is set in the
    process environment, else `default`."""
    env_var = f"{ENV_PREFIX}{field_name.upper()}"
    return "env" if env_var in os.environ and os.environ[env_var] != "" else "default"


def _stringify_default(field_info: FieldInfo) -> str:
    """Stringify the schema default. For SecretStr defaults we use the
    empty string (the default *value* is "", not the mask)."""
    default = field_info.default
    if default is None:
        return ""
    if isinstance(default, SecretStr):
        secret = default.get_secret_value()
        # Keep parity with `_stringify`: mask when non-empty so the page
        # never leaks even a baked-in placeholder secret.
        return MASK if secret else ""
    if callable(default):
        # `default_factory` — we don't invoke it; just signal it's dynamic.
        return "<computed>"
    return _stringify(default)


def describe_settings(settings: Settings) -> list[dict[str, Any]]:
    """Build the per-key metadata + value list. Sensitive values are
    replaced with the mask before this function returns."""
    allowlisted = set(allowlist())
    out: list[dict[str, Any]] = []
    for name, field_info in Settings.model_fields.items():
        extra = field_info.json_schema_extra
        meta: dict[str, Any] = extra if isinstance(extra, dict) else {}
        sensitive = _is_sensitive(field_info)

        raw = getattr(settings, name)
        rendered = _stringify(raw)
        if sensitive and rendered and rendered != MASK:
            rendered = MASK

        out.append(
            {
                "key": name,
                "value": rendered,
                "sensitive": sensitive,
                "default": _stringify_default(field_info),
                "source": _source(name),
                "description": field_info.description or "",
                "impact": meta.get("impact", ""),
                "category": meta.get("category", ""),
                "stability": meta.get("stability", ""),
                "mutable": name in allowlisted,
                "restart": _restart_impact(name, allowlisted),
                "range": _range_text(field_info),
            }
        )
    return out


@router.get(
    "/sys/config",
    summary="GET gateway configuration with per-key metadata (read-only)",
)
async def get_sys_config(request: Request) -> dict[str, Any]:
    settings: Settings = request.app.state.settings
    return {
        "entries": describe_settings(settings),
        "scope": "gateway",
        "loaded_at": _LOADED_AT,
        "request_id": request.state.request_id,
    }
