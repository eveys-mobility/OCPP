from __future__ import annotations

from datetime import date

project = "eveys/ocpp"
author = "Eveys engineering"
copyright = f"{date.today().year}, {author}"

extensions = [
    "myst_parser",
    "sphinx_copybutton",
    "sphinx.ext.autosectionlabel",
]

myst_enable_extensions = [
    "colon_fence",
    "deflist",
    "linkify",
    "tasklist",
]

myst_heading_anchors = 3
autosectionlabel_prefix_document = True

source_suffix = {".md": "markdown"}
master_doc = "README"

exclude_patterns = [
    "_build",
    ".venv",
    "Thumbs.db",
    ".DS_Store",
    "requirements.txt",
    "Makefile",
    "adr/template.md",
]

html_theme = "furo"
html_title = "eveys/ocpp documentation"
html_static_path = ["_static"]

suppress_warnings = ["myst.header"]
