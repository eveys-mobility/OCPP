"""Inbound REST API package (E3-7, ADR-0026).

The gateway exposes a read REST surface at `/api/v1/...` (this MR)
and a command surface at the same prefix (E3-8, later).

Module layout:

- `_app.py` — FastAPI app factory; assembles routers + middleware.
- `_auth.py` — bearer-token validation against the inbound allowlist.
- `_errors.py` — error envelope handler + typed `ApiError`.
- `_pagination.py` — opaque base64-JSON cursor encode/decode.
- `health.py` — `GET /api/v1/health`.
- `charge_points.py`, `transactions.py`, ... — per-domain routers.

The ASGI server lifecycle (uvicorn) lives in
`eveys_ocpp.transport.rest_server`, mirroring the WS + gRPC transports.
"""
