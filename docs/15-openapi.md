# OpenAPI / Swagger

The gateway's REST API has two complementary specifications:

| Form | Where | When to use it |
| --- | --- | --- |
| **Hand-written contract** | [`docs/integration/02-gateway-rest-api.md`](./integration/02-gateway-rest-api.md) | Source-of-truth prose. Read this first to understand intent (auth, pagination, error envelope, contract guarantees). |
| **Generated OpenAPI 3.1 spec** | [`docs/api/openapi.yaml`](./api/openapi.yaml) + `openapi.json` | Import into Postman / Insomnia / external Swagger UIs. Regenerated from the live FastAPI app via `make openapi-export`. |
| **Runtime Swagger UI** | `/api/v1/docs` (also `/api/v1/redoc`, `/api/v1/openapi.json`) | Dev/staging only. Click "Try it out" in a browser. **Off by default** — see [Security note](#security-note) below. |

## Quickstart — runtime Swagger UI

```bash
# 1. Bring up the local stack with OpenAPI on
EVEYS_OCPP_REST_OPENAPI_ENABLED=true \
EVEYS_OCPP_REST_AUTH_DISABLED=true \
make compose-up

# 2. Open Swagger UI in a browser
open http://localhost:8080/api/v1/docs

# 3. Or hit ReDoc if you prefer that style
open http://localhost:8080/api/v1/redoc

# 4. Or grab the raw spec
curl http://localhost:8080/api/v1/openapi.json | jq .
```

Auth still applies to `/api/v1/openapi.json`, `/api/v1/docs`, and `/api/v1/redoc` — only token-bearers can read the spec. Use `EVEYS_OCPP_REST_AUTH_DISABLED=true` for laptop dev (the boot-time `rest_auth.disabled=True` log line makes a forgotten flip obvious in any review).

## Quickstart — static spec

```bash
# Regenerate from the live FastAPI app — no runtime needed
make openapi-export

# Files written
ls docs/api/
# openapi.json
# openapi.yaml

# Import into Postman: File → Import → docs/api/openapi.yaml
```

CI runs `make openapi-export-check` to fail builds where the committed file drifts from the FastAPI app — same pattern as `config-export-check`.

## Quickstart — static Swagger UI site

A `make` target ships a self-contained Swagger UI site rendered from the committed `docs/api/openapi.json`. No Docker, no compose, no gateway running — just static HTML/CSS/JS you can serve locally or copy to any web host.

```bash
cd docs
make swagger          # build static site to docs/_build/swagger/
make swagger-serve    # build (if needed) + serve on http://localhost:8000
```

`make swagger-serve` blocks the terminal while the server runs. Stop with `Ctrl+C`. Override the port via `make swagger-serve SWAGGER_PORT=8765` if 8000 is taken.

The output at `docs/_build/swagger/` is a flat directory of HTML/CSS/JS that you can:

- copy to S3 / GitHub Pages / an internal nginx for a persistent URL
- `rsync` to a server on your LAN
- open `index.html` directly in a browser (most browsers block `fetch("openapi.json")` from a `file://` URL, so the served path is the reliable option)

The Swagger UI version is pinned in `docs/Makefile` (`SWAGGER_UI_VERSION`); bump it deliberately and re-run `make swagger`.

The spec served here is the **same** `docs/api/openapi.json` that `make openapi-export` regenerates from the live FastAPI app — there's only ever one source of truth.

### When to use which surface

| Surface | When |
|---|---|
| `make swagger-serve` (this PR) | Browse the contract without a running gateway. Cheapest UI. |
| Runtime Swagger UI (`/api/v1/docs` on a running gateway) | Verify *this specific deploy*'s contract — useful when investigating drift between an old build and the latest spec. |
| `docs/api/openapi.yaml` import to Postman | Generate a request collection for manual testing. |
| `editor.swagger.io` paste | Quick view from any laptop without cloning the repo. |

## What's in the spec

- **All 28 routes** under `/api/v1/`: health, charge-points (list + detail), transactions (list + detail), reservations, charging-profiles, time-series (meter-values + status-history), and 19 commands.
- **21 named schemas** — the headline shapes (`ChargePoint`, `Transaction`, `Reservation`, `ChargingProfile`, `MeterValueSample`, `StatusEvent`, `HealthResponse`, `ErrorEnvelope`) plus paginated wrappers and three command request bodies (`RemoteStartRequest`, `RemoteStopRequest`, `ResetRequest`).
- **Examples** — every named response model carries a `model_config["json_schema_extra"]["example"]`, so Swagger UI's "Try it out" pre-fills with a realistic payload.

## What's intentionally not in the spec yet

The 19 command endpoints fall in two tiers:

- **Typed**: `RemoteStart`, `RemoteStop`, `Reset` — request bodies are full Pydantic models with examples.
- **Generic**: the other 16 (`ChangeConfiguration`, `TriggerMessage`, `ReserveNow`, `SetChargingProfile`, …) advertise the standard `CommandAcceptedResponse` shape but treat their request body as `application/json` without a strict schema. Each one's contract is in [`docs/integration/02-gateway-rest-api.md`](./integration/02-gateway-rest-api.md) — typifying them is a follow-up cleanup.

The three list/detail read endpoints (`GET /charge-points`, `GET /charge-points/{cp_id}`, `GET /transactions/{transaction_id}`) return plain dicts at runtime; their schemas are declared via `responses=` annotations rather than `response_model=`, so OpenAPI describes the shape **without** forcing runtime validation that could break the production endpoint. The trade-off: schemas can drift from reality if a route's response shape changes without a corresponding model edit. The `tests/unit/api/test_openapi.py` snapshot test catches drift on CI; reviewers spot the rest.

## Security note

ADR-0026 originally disabled the OpenAPI surface entirely (`docs_url=None, redoc_url=None, openapi_url=None`) so the gateway wouldn't self-publish a discoverable schema. That decision is preserved as the default — `EVEYS_OCPP_REST_OPENAPI_ENABLED=false` (which is the default) keeps the surface locked down.

Flipping the toggle on in production widens the attack surface: anyone with a valid bearer token can read the full schema and use it to find endpoints they didn't otherwise know about. For staging and dev that's a reasonable trade for operator convenience; for production, **prefer the static spec** (`docs/api/openapi.{json,yaml}`) shared via internal channels.

A boot-time WARNING log fires when the toggle is on:

```
warning rest_openapi.enabled detail='EVEYS_OCPP_REST_OPENAPI_ENABLED=True — the gateway is publishing OpenAPI schema + Swagger UI on `/api/v1/`. ...'
```

Grep for `rest_openapi.enabled` in production logs to catch a misconfigured deploy.

## Postman collection

The static `openapi.yaml` imports cleanly into Postman: **File → Import → upload-file**. Postman generates a collection with all 28 routes; set up an environment with:

| Var | Example |
| --- | --- |
| `base_url` | `http://localhost:8080` |
| `bearer_token` | `dev-token` (matches `EVEYS_OCPP_REST_INBOUND_TOKENS`) |

Then add a top-level Authorization → Bearer Token → `{{bearer_token}}` so every request inherits the header.

For raw `curl`, see the per-endpoint examples in [`12-connecting-real-charger.md`](./12-connecting-real-charger.md).
