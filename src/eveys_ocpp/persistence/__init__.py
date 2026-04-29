"""Postgres persistence layer.

`db.py`           — engine + session factory (async).
`models.py`       — SQLAlchemy ORM models.
`repositories.py` — query functions, the only thing handlers call directly.
"""

from __future__ import annotations
