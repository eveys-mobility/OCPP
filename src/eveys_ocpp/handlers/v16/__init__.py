"""OCPP 1.6 handlers.

Hard rule: this package never imports from `ocpp.v201`, `ocpp.v21`, or our
own `handlers.v201`. See AGENTS.md OCPP rule 1 — cross-version imports
silently produce invalid messages on the wire.
"""

from __future__ import annotations
