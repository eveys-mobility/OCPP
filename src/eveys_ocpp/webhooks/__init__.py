"""Outbound webhook delivery (E3-9).

The gateway tails its own Kafka event topics, signs each event with
HMAC-SHA-256, and POSTs to backend-configured URLs. Per-event toggles
in `Settings` decide which events leave the gateway as webhooks; the
default is everything except `cp.meter` (high volume — Kafka is the
right channel for that one).

Public entry point: `WebhookDispatcher`. Construct it in `__main__.py`
when `webhook_base_url` is configured, run `serve_forever()` in the
existing TaskGroup.

Spec: `docs/integration/03-webhooks.md`.
"""

from eveys_ocpp.webhooks.backlog_drainer import WebhookBacklogDrainer
from eveys_ocpp.webhooks.dispatcher import WebhookDispatcher
from eveys_ocpp.webhooks.signer import compute_signature, verify_signature

__all__ = [
    "WebhookBacklogDrainer",
    "WebhookDispatcher",
    "compute_signature",
    "verify_signature",
]
