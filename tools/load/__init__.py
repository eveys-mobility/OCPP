"""Load test rig — drive `tools.sim` at scale, capture pass/fail evidence.

Standalone tool, NOT shipped in the production wheel — same gating as
`tools/sim` (lives outside `src/eveys_ocpp/`, no `[project.scripts]`
entry). Invoke via `python -m tools.load ...` from a dev checkout.

Architecture:

  Scenarios (one per pass criterion) drive the simulator with a
  scenario-specific shape, then evaluate Prometheus over the run
  window to decide pass/fail. Each scenario emits a `ScenarioResult`
  with one `Criterion` per checkable property; the report renderer
  turns N `ScenarioResult`s into a Markdown report a reviewer can
  drop into a PR or wiki.

  Out of scope for v0 (deferred follow-ups):
   - Multi-machine simulator orchestration (`run.sh` runs the
     simulator on the same box that runs the report)
   - k3d / kind cluster targeting (compose-only for now)
   - Grafana screenshot rendering (Markdown links to dashboard URLs
     instead of inlining PNGs)
"""

from __future__ import annotations

__version__ = "0.1.0"
