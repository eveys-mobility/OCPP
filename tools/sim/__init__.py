"""eveys-ocpp-sim — virtual charger fleet for load and lifecycle testing.

Standalone tool, NOT shipped in the production wheel — lives at the
repo top level under `tools/sim/` instead of `src/eveys_ocpp/`. The
`pyproject.toml` `[project.scripts]` entry is gated by the dev extras
so a production install doesn't expose `eveys-ocpp-sim`.

Used by E4-6 (load test rig) and E4-7 (reconnect-storm test); also a
useful dev affordance — `eveys-ocpp-sim --count 10 --duration 60` is
the new "open browser to verify the gateway is alive".
"""

from __future__ import annotations

__version__ = "0.1.0"
