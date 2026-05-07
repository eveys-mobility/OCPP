"""CLI: `python -m tools.load --quick` or `--scenario <name>`.

`--quick` runs all scenarios with their `run_quick` shape (under 2
minutes total against `make compose-up`). `--full` runs the
production-shaped `run_full` shape (long-running, requires staging).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys

from tools.load.report import render_markdown
from tools.load.scenario import ScenarioResult
from tools.load.scenarios import boot_storm

# Closed registry of scenarios. Adding a new scenario means a one-line
# edit here plus a new module under `tools/load/scenarios/`.
_SCENARIOS = {
    "boot_storm": boot_storm,
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tools.load",
        description=(
            "Drive the simulator (`tools.sim`) at scale and emit a "
            "pass/fail Markdown report. v0 ships one scenario "
            "(`boot_storm`); add more under `tools/load/scenarios/`."
        ),
    )
    parser.add_argument(
        "--target",
        default="ws://localhost:19000",
        help="Gateway WS URL (default: ws://localhost:19000).",
    )
    parser.add_argument(
        "--prometheus",
        default="http://localhost:9090",
        help=(
            "Prometheus base URL (default: http://localhost:9090). "
            "The compose stack does not run Prometheus itself; point "
            "this at whatever scraper your operator wired up."
        ),
    )
    parser.add_argument(
        "--scenario",
        choices=sorted(_SCENARIOS.keys()),
        action="append",
        help="Scenario name. Repeat to run multiple. Default: all.",
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use each scenario's small/fast shape (under 2 minutes total).",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use each scenario's production-shaped shape (long-running).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON to stdout instead of Markdown.",
    )
    parser.add_argument(
        "--out",
        type=str,
        default=None,
        help="Write the report to this path; default writes to stdout.",
    )
    return parser


async def _run_all(
    scenario_names: list[str], *, target: str, prometheus: str, quick: bool
) -> list[ScenarioResult]:
    results: list[ScenarioResult] = []
    for name in scenario_names:
        module = _SCENARIOS[name]
        runner = module.run_quick if quick else module.run_full
        results.append(await runner(target, prometheus))
    return results


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.quick and args.full:
        print("--quick and --full are mutually exclusive", file=sys.stderr)
        return 2
    quick = args.quick or not args.full  # default to quick
    scenarios = args.scenario or sorted(_SCENARIOS.keys())

    results = asyncio.run(
        _run_all(scenarios, target=args.target, prometheus=args.prometheus, quick=quick)
    )
    if args.json:
        rendered = json.dumps([r.to_dict() for r in results], indent=2)
    else:
        rendered = render_markdown(results)
    if args.out:
        with open(args.out, "w") as fp:
            fp.write(rendered)
    else:
        print(rendered)

    # Exit non-zero if any scenario failed — lets a CI smoke wrap the
    # rig with `set -e` and have it surface real failures.
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
