"""Mock backend behaviour controls.

Reads env vars with safe defaults so the mock works out of the box
for `python -m tests.mock_backend` runs and for in-process pytest
fixtures alike.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class MockBackendConfig:
    """Frozen config snapshot taken at app build time.

    Tests build a custom config with overrides via ``build_app``;
    standalone deployments read env vars via ``from_env``.
    """

    bearer_token: str = "dev-token"
    blocked_id_tags: frozenset[str] = field(default_factory=frozenset)
    fail_authorize: bool = False
    heartbeat_interval_seconds: int = 60
    # Optional override that makes /authorize always return Accepted
    # for any id_tag, even ones in `blocked_id_tags` (test convenience
    # for happy-path runs).
    force_accept_all: bool = False

    @classmethod
    def from_env(cls) -> MockBackendConfig:
        return cls(
            bearer_token=os.environ.get("MOCK_BACKEND_TOKEN", "dev-token"),
            blocked_id_tags=frozenset(
                t.strip()
                for t in os.environ.get("MOCK_BACKEND_BLOCKED_ID_TAGS", "").split(",")
                if t.strip()
            ),
            fail_authorize=os.environ.get("MOCK_BACKEND_FAIL_AUTHORIZE") == "1",
            heartbeat_interval_seconds=int(
                os.environ.get("MOCK_BACKEND_HEARTBEAT_INTERVAL_SECONDS", "60")
            ),
            force_accept_all=os.environ.get("MOCK_BACKEND_FORCE_ACCEPT_ALL") == "1",
        )
