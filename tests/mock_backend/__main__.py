"""CLI entry point — boot the mock backend with uvicorn.

    python -m tests.mock_backend [--host 0.0.0.0] [--port 9100]

Reads behaviour controls from env vars (see `tests.mock_backend`
docstring for the full list).
"""

from __future__ import annotations

import argparse
import os

import uvicorn


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock Eveys backend (E3-10)")
    parser.add_argument(
        "--host",
        default=os.environ.get("MOCK_BACKEND_HOST", "0.0.0.0"),
        help="bind host (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MOCK_BACKEND_PORT", "9200")),
        help="bind port (default: 9200)",
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("MOCK_BACKEND_LOG_LEVEL", "info"),
        help="uvicorn log level",
    )
    args = parser.parse_args()

    # Use the import path so uvicorn picks up the module-level `app`
    # built from env-driven `MockBackendConfig.from_env()`.
    uvicorn.run(
        "tests.mock_backend.app:app",
        host=args.host,
        port=args.port,
        log_level=args.log_level,
    )


if __name__ == "__main__":
    main()
