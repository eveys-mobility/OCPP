"""Generate `docs/11-configuration-reference.md` and `.env.example` from `Settings`.

Per ADR-0025, the Pydantic `Settings` model is the source of truth for
operator-facing configuration documentation. This script walks
`Settings.model_fields`, groups by `json_schema_extra["category"]`, and
emits two artefacts:

  1. `docs/11-configuration-reference.md` — operator-facing reference.
  2. `.env.example` — copy-paste starter for `.env`, with secrets blanked.

The renderer is a pure function (`render_config_reference`). The
`__main__` block writes the two strings to disk; the `--check` flag
re-renders and exits non-zero if the committed files differ from the
generator's output. CI uses the same flag (E0-14) to refuse drift.
"""

from __future__ import annotations

import argparse
import sys
import typing
from pathlib import Path
from typing import Final

from annotated_types import Ge, Le, MaxLen, MinLen
from pydantic.fields import FieldInfo
from pydantic_settings import BaseSettings

# Repo-root-relative paths the script writes to.
REPO_ROOT: Final = Path(__file__).resolve().parent.parent
DOC_PATH: Final = REPO_ROOT / "docs" / "11-configuration-reference.md"
ENV_EXAMPLE_PATH: Final = REPO_ROOT / ".env.example"

# U+2013 EN DASH, used in range cells to match the hand-written seed's
# typography. Spelled via escape so ruff's RUF001 (ambiguous-character)
# check stays quiet on the literal.
_EN_DASH: Final = "\u2013"

# Closed list of categories from ADR-0025. Order here is the order of
# H2 sections in the rendered doc. Adding a category is a one-line edit
# here plus the matching entry in `_SECTION_TITLES`.
_CATEGORY_ORDER: Final[tuple[str, ...]] = (
    "ws_server",
    "grpc_server",
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
)

# Human-readable H2 heading per category. Mirrors the hand-written seed.
_SECTION_TITLES: Final[dict[str, str]] = {
    "ws_server": "WS server",
    "grpc_server": "gRPC server",
    "kafka_producer": "Kafka producer (ADR-0019)",
    "kafka_topics": "Kafka topics",
    "redis": "Redis (online registry + pub/sub bus, ADR-0016)",
    "postgres": "Postgres",
    "identity": "Identity (Kubernetes downward-API)",
    "logging": "Logging",
    "ocpp_defaults": "OCPP defaults",
    "cross_pod_bus": "Cross-pod command bus (ADR-0016)",
    "idempotency": "Idempotency cache (E2-11)",
    "clickhouse_ingest": "ClickHouse ingestion sidecar (ADR-0020)",
    "backend_integration": "Backend integration (ADR-0023, E3-2..E3-6)",
    "authorize_cache": "Authorize cache (E3-4)",
}

# Optional per-section blockquote shown immediately under the H2.
_SECTION_PREAMBLES: Final[dict[str, str]] = {
    "kafka_topics": (
        "> The four topic names are part of the **frozen v1 contract** with "
        "downstream consumers (per `proto/events/v1/events.proto` and "
        "ADR-0018). Treat them as structural — renaming is an externally "
        "visible breaking change."
    ),
    "clickhouse_ingest": (
        "> The ingestor is a separate process (`python -m "
        "eveys_ocpp.clickhouse.ingestor`); these settings configure it but "
        "the gateway itself does not connect to ClickHouse."
    ),
}

_HEADER: Final = (
    (
        "# Configuration reference\n"
        "\n"
        "> **Source of truth:** `src/eveys_ocpp/settings.py`. Per "
        "[ADR-0025](./adr/0025-generated-config-reference.md),\n"
        "> this page is **regenerated** from that file by\n"
        "> `scripts/render_config_reference.py`. Do not hand-edit — change the\n"
        "> Pydantic field instead and run `make config-export`.\n"
    )
    + """
Every variable below is read from the environment with prefix
`EVEYS_OCPP_` (e.g. `EVEYS_OCPP_LOG_LEVEL`). Defaults match
`Settings()` field defaults. Ranges come from the field's Pydantic
constraints (`ge=`, `le=`, `pattern=`) or `Literal[...]` alternatives.

**Stability column** answers "what happens if I change this":

- **tunable** — operator-facing, safe to change at runtime; no
  schema / wire-format consequences. Restart the service after the
  change so it picks the new value up.
- **structural** — changes how the service binds to the world.
  Coordinate with whoever else uses these ports / topics / DSNs.
- **dev-only** — local stacks and tests; do not set in production.

**Secret column** flags sensitive values; do not log, do not commit
to a values file, prefer your secrets manager. Phase 5 vault work
(E5-7) moves these to `SecretStr`; until then operators handle the
sensitivity.

---

"""
)

_FOOTER: Final = """
---

## Common operations

### "I want to read every variable on a running container."

```bash
docker exec eveys-ocpp env | grep '^EVEYS_OCPP_'
```

### "I want a starter `.env`."

`make config-export` regenerates `.env.example` alongside this file;
`cp .env.example .env` and edit. Secrets are blank in the example.

### "I changed a variable; do I need to redeploy?"

`Settings` is read once at process start (`get_settings()`). Yes —
restart the gateway pod (rolling restart in k8s) for a new value to
take effect. There is no live-reload path.

### "Which variables are sensitive?"

Anything tagged **secret = yes** in the tables above. Today that's
`BACKEND_TOKEN` and the password embedded in `DB_URL`. Phase 5 vault
work moves both to a secrets manager.

### "Where do I read the live values for an incident?"

`/health` will return the non-secret slice once E4-* (observability
phase) lands — until then, `docker exec ... env` is the answer.
"""


def _is_secret(info: FieldInfo) -> bool:
    extra = info.json_schema_extra
    if not isinstance(extra, dict):
        return False
    return bool(extra.get("secret"))


def _category(info: FieldInfo) -> str:
    extra = info.json_schema_extra
    assert isinstance(extra, dict), "field missing json_schema_extra (caught by metadata test)"
    return str(extra["category"])


def _impact(info: FieldInfo) -> str:
    extra = info.json_schema_extra
    assert isinstance(extra, dict)
    return str(extra["impact"])


def _stability(info: FieldInfo) -> str:
    extra = info.json_schema_extra
    assert isinstance(extra, dict)
    return str(extra["stability"])


def _is_dynamic_default(info: FieldInfo) -> bool:
    return info.default_factory is not None


def _format_default_for_doc(info: FieldInfo) -> str:
    """Render the default for the markdown table cell."""
    if _is_dynamic_default(info):
        # Avoid calling the factory at render time — would write the
        # dev's hostname into the committed doc.
        return "hostname"
    default = info.default
    if default is None:
        return "(none)"
    if isinstance(default, str):
        if default == "":
            return "(empty)"
        return f"`{default}`"
    if isinstance(default, bool):
        return f"`{str(default).lower()}`"
    return f"`{default}`"


def _format_range(info: FieldInfo) -> str:
    """Render the range cell from Pydantic v2 metadata or `Literal[...]`.

    For `Literal[a, b, c]`, the alternatives are joined by ` / `. For
    numerics, `Ge`/`Le` produce `lo-hi`. For free strings without
    constraints, return a short type label like `string` or `URL`.
    """
    annotation = info.annotation
    origin = typing.get_origin(annotation)

    if origin is typing.Literal:
        values = typing.get_args(annotation)
        return " / ".join(f"`{v}`" for v in values)

    if annotation is bool:
        return "bool"

    lo: float | int | None = None
    hi: float | int | None = None
    min_len: int | None = None
    max_len: int | None = None
    pattern: str | None = None
    for constraint in info.metadata:
        if isinstance(constraint, Ge):
            lo = constraint.ge  # type: ignore[assignment]
        elif isinstance(constraint, Le):
            hi = constraint.le  # type: ignore[assignment]
        elif isinstance(constraint, MinLen):
            min_len = constraint.min_length
        elif isinstance(constraint, MaxLen):
            max_len = constraint.max_length
        elif hasattr(constraint, "pattern"):
            pattern = str(constraint.pattern)

    # Ranges use a typographic en-dash to match the hand-written seed.
    if lo is not None and hi is not None:
        return f"{lo}{_EN_DASH}{hi}"
    if lo is not None:
        return f">= {lo}"
    if hi is not None:
        return f"<= {hi}"
    if min_len is not None or max_len is not None:
        upper = max_len if max_len is not None else "inf"
        return f"len {min_len or 0}{_EN_DASH}{upper}"
    if pattern is not None:
        return f"matches `{pattern}`"

    if annotation is str:
        return "string"
    if annotation is int:
        return "integer"
    if annotation is float:
        return "float"
    return "—"


def _env_var_name(field_name: str) -> str:
    return f"EVEYS_OCPP_{field_name.upper()}"


def _escape_pipe(s: str) -> str:
    """Escape `|` so it doesn't break Markdown table cells."""
    return s.replace("|", "\\|")


def _render_field_row(field_name: str, info: FieldInfo) -> str:
    var = _env_var_name(field_name)
    default = _format_default_for_doc(info)
    rng = _format_range(info)
    stability = _stability(info)
    secret = "**yes**" if _is_secret(info) else "no"
    description = _escape_pipe((info.description or "").strip())
    impact = _escape_pipe(_impact(info).strip())
    return f"| `{var}` | {default} | {rng} | {stability} | {secret} | {description} | {impact} |"


def _render_section(category: str, fields: list[tuple[str, FieldInfo]]) -> str:
    lines: list[str] = [f"## {_SECTION_TITLES[category]}", ""]
    if category in _SECTION_PREAMBLES:
        lines.extend([_SECTION_PREAMBLES[category], ""])
    header_row = (
        "| Variable | Default | Range | Stability | Secret | What it does | Impact of changing |"
    )
    lines.extend([header_row, "|---|---|---|---|---|---|---|"])
    lines.extend(_render_field_row(name, info) for name, info in fields)
    lines.append("")
    return "\n".join(lines)


def _group_fields_by_category(
    settings_cls: type[BaseSettings],
) -> dict[str, list[tuple[str, FieldInfo]]]:
    grouped: dict[str, list[tuple[str, FieldInfo]]] = {c: [] for c in _CATEGORY_ORDER}
    for name, info in settings_cls.model_fields.items():
        category = _category(info)
        if category not in grouped:
            raise ValueError(
                f"Field {name!r} has unknown category {category!r}; "
                f"add it to _CATEGORY_ORDER in {Path(__file__).name} "
                f"or pick one of: {sorted(grouped)}"
            )
        grouped[category].append((name, info))
    return grouped


def _render_markdown(settings_cls: type[BaseSettings]) -> str:
    grouped = _group_fields_by_category(settings_cls)
    section_blocks: list[str] = []
    for category in _CATEGORY_ORDER:
        fields = grouped[category]
        if not fields:
            # Categories listed in the closed enum but unused are skipped
            # silently. Adding fields later picks up automatically.
            continue
        section_blocks.append(_render_section(category, fields))
    body = "\n".join(section_blocks)
    return _HEADER + body + _FOOTER


def _format_default_for_env(info: FieldInfo) -> str | None:
    """Return the value to put on the right-hand side of `KEY=` in `.env.example`.

    `None` means the line should be a blank value. The caller decides
    whether to add a comment.
    """
    if _is_dynamic_default(info):
        return None
    default = info.default
    if default is None or default == "":
        return None
    if isinstance(default, bool):
        return str(default).lower()
    return str(default)


def _render_env_line(field_name: str, info: FieldInfo) -> str:
    var = _env_var_name(field_name)
    if _is_secret(info):
        # Always blank secrets in .env.example, even if the default is a
        # placeholder. The committed file is checked in; embedding even a
        # dev-grade secret value defeats the whole point of the flag.
        return f"# secret — fill from your secrets manager\n{var}="
    if _is_dynamic_default(info):
        return (
            f"# dynamic: defaults to a runtime value (e.g. socket.gethostname()); "
            f"set explicitly in production\n{var}="
        )
    value = _format_default_for_env(info)
    if value is None:
        return f"{var}="
    return f"{var}={value}"


def _render_env_section(category: str, fields: list[tuple[str, FieldInfo]]) -> str:
    title = _SECTION_TITLES[category]
    lines = [f"# ---- {title} " + "-" * max(0, 60 - len(title)), ""]
    for name, info in fields:
        lines.append(_render_env_line(name, info))
    lines.append("")
    return "\n".join(lines)


def _render_env_example(settings_cls: type[BaseSettings]) -> str:
    grouped = _group_fields_by_category(settings_cls)
    blocks: list[str] = [
        "# Generated from src/eveys_ocpp/settings.py by",
        "# scripts/render_config_reference.py. Do not hand-edit; change",
        "# the Settings field and run `make config-export`.",
        "#",
        "# Copy to .env (`cp .env.example .env`) and fill in secrets.",
        "",
    ]
    for category in _CATEGORY_ORDER:
        fields = grouped[category]
        if not fields:
            continue
        blocks.append(_render_env_section(category, fields))
    return "\n".join(blocks).rstrip() + "\n"


def render_config_reference(
    settings_cls: type[BaseSettings],
) -> tuple[str, str]:
    """Return `(markdown, env_example)` for the given Settings class.

    Pure function — no I/O, no env reads, no clock reads. Deterministic:
    same class → byte-identical output. Used directly by tests; the
    `__main__` block writes the two strings to disk.
    """
    return _render_markdown(settings_cls), _render_env_example(settings_cls)


def _diff_or_zero(label: str, generated: str, on_disk: Path) -> int:
    if not on_disk.exists():
        sys.stderr.write(f"{label}: {on_disk} does not exist; run without --check.\n")
        return 1
    current = on_disk.read_text()
    if current == generated:
        return 0
    sys.stderr.write(
        f"{label}: {on_disk} is out of date.\n  Run `make config-export` to regenerate.\n"
    )
    return 1


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the configuration reference doc and .env.example "
            "from src/eveys_ocpp/settings.py."
        )
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help=(
            "Exit non-zero if the generated output differs from the "
            "committed files. CI uses this (E0-14)."
        ),
    )
    args = parser.parse_args(argv)

    # Local import keeps the module importable in environments that
    # haven't installed the project (e.g. linting standalone).
    from eveys_ocpp.settings import Settings

    markdown, env_example = render_config_reference(Settings)

    if args.check:
        rc = 0
        rc |= _diff_or_zero("docs/11-configuration-reference.md", markdown, DOC_PATH)
        rc |= _diff_or_zero(".env.example", env_example, ENV_EXAMPLE_PATH)
        if rc == 0:
            print("Config reference is up to date.")
        return rc

    DOC_PATH.write_text(markdown)
    ENV_EXAMPLE_PATH.write_text(env_example)
    print(f"Wrote {DOC_PATH}")
    print(f"Wrote {ENV_EXAMPLE_PATH}")
    return 0


__all__ = [
    "DOC_PATH",
    "ENV_EXAMPLE_PATH",
    "render_config_reference",
]


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
