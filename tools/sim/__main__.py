"""CLI entry point — `python -m tools.sim` or `eveys-ocpp-sim`.

Parses arguments, builds a `Fleet`, runs it inside `asyncio.run`.
Exit code is 0 when the fleet ran cleanly (errors counter doesn't
drive exit — load tests intentionally observe failures), 130 on
Ctrl-C.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from tools.sim.fleet import Fleet, FleetConfig
from tools.sim.profiles import PROFILES, get_profile


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eveys-ocpp-sim",
        description=(
            "Run N virtual OCPP 1.6 chargers against a target gateway. "
            "Used by E4-6 load test, E4-7 reconnect-storm test, and as "
            "a dev affordance for smoke-testing changes."
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of virtual chargers to run (default: 10).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Run for N seconds, then exit (default: 60).",
    )
    parser.add_argument(
        "--target",
        type=str,
        default="ws://localhost:19000",
        help=(
            "Gateway WS URL prefix; the per-charger `cp_id` is appended "
            "as a path segment. Default: `ws://localhost:19000` "
            "(matches docker-compose.yml)."
        ),
    )
    parser.add_argument(
        "--ramp-seconds",
        type=float,
        default=10.0,
        help=(
            "Spread connect timing across N seconds so the fleet doesn't "
            "all hit the gateway in the same millisecond (default: 10)."
        ),
    )
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES.keys()),
        default="realistic",
        help="Behaviour preset for every charger in the fleet.",
    )
    parser.add_argument(
        "--cp-id-prefix",
        type=str,
        default="SIM",
        help="Prefix for the generated `cp_id` values (default: `SIM`).",
    )
    parser.add_argument(
        "--no-progress",
        action="store_true",
        help="Suppress the per-second status line on stderr.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = FleetConfig(
        count=args.count,
        duration_seconds=args.duration,
        target_url=args.target,
        ramp_seconds=args.ramp_seconds,
        profile=get_profile(args.profile),
        cp_id_prefix=args.cp_id_prefix,
        show_progress=not args.no_progress,
    )
    fleet = Fleet(config)
    try:
        asyncio.run(fleet.run())
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":  # pragma: no cover — exercised via the script entry
    sys.exit(main())
