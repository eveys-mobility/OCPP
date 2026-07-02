# 18 — Charger-rollback runbook

> **What this is.** The operator-facing playbook for cutting **one
> specific charger** off the gateway under pressure. Phase 6 / E6
> calls for a drill that disconnects a charger from `ocpp-gw` in
> under 2 minutes; this is the runbook the drill validates.
>
> **What this is NOT.** A fleet-wide rollback — that's a Helm /
> k8s rollout concern, separate doc. A protocol-level
> ["RemoteStop the active session"](./integration/02-gateway-rest-api.md#post-apiv1charge-pointscp_idcommandsremote-stop)
> command — that ends the session politely; this runbook ends the
> *connection*. A platform incident response — page on-call first,
> then refer to this if a single rogue charger is the cause.

## When to invoke this

A single charger needs to be cut off **right now**, not gracefully:

- **Vendor firmware bug** — charger is in a reboot loop, flooding logs, blowing the per-charger rate-limit budget.
- **PII / compliance leak** — charger is sending data it shouldn't (vendor MeterValues with customer fields, `vendor_error_code` containing card numbers, etc.).
- **Stolen credential suspected** — the charger's `cp_id` is connecting from an unexpected IP or in a way that suggests credential reuse.
- **Stuck session** — `RemoteStop` was tried, charger didn't respond, transaction is open and metering wrong values.

If the issue is fleet-wide (every charger) or platform-wide (gateway crashing), this is the wrong runbook — see [`16-dr-runbook.md`](./16-dr-runbook.md) and the on-call playbook (Phase 7).

## The 2-minute budget

The roadmap target is "**< 2 minutes** from decision to charger-disconnected". Each lever below has a time budget; the levers compose, but the operator should hit the first one that closes the window — not all four.

| Step | Lever | Time | Reversibility |
|---|---|---|---|
| 1 | Rotate password (deny next reconnect) | ~15 s | Reversible — restore the old hash |
| 2 | `Reset.Hard` (force charger to reboot, reconnect attempt then 401s on step 1) | ~30 s after step 1 | Charger reconnects on its own once you reverse step 1 |
| 3 (optional) | Network-edge IP block | ~30 s | Operator-controlled |
| 4 (last resort) | Pod-kill the gateway pod the charger is on | ~60 s | Charger lands on a sibling pod |

Steps 1+2 are the standard play. Step 3 is for malicious chargers (bad actor). Step 4 is for the rare case where the charger is somehow re-authenticating despite step 1 (only plausible if step 1 didn't actually take — see § "When step 1 doesn't stick").

## Step 1 — Rotate the per-charger password

The lever: change `charge_point_credentials.password_hash` for the
target charger to a value the charger doesn't know. Its current
WebSocket connection survives (Basic Auth is upgrade-time only);
its **next reconnect attempt fails 401**, which combined with step
2 below produces a hard disconnect within ~30 s.

> **No REST endpoint yet.** The credential-rotation REST endpoint
> is forthcoming; until it lands, operators rotate via direct SQL.
> The model docstring at
> [`persistence/models.py`](../src/eveys_ocpp/persistence/models.py)
> § `ChargePointCredential` covers the schema.

### 1a. Generate a fresh bcrypt hash

Any bcrypt-producing tool works. Most operator workstations have one of:

```bash
# Python (always available where the gateway is deployed):
python3 -c "import bcrypt; print(bcrypt.hashpw(b'<random>', bcrypt.gensalt()).decode())"

# Or htpasswd, if installed:
htpasswd -nbB CP_TARGET '<random>' | cut -d: -f2
```

The plaintext is throwaway — you're rotating the credential, not setting a new working password. Use `openssl rand -hex 16` or anything you'll discard.

### 1b. UPDATE the row

```sql
UPDATE charge_point_credentials
   SET password_hash = '<new-bcrypt-hash>',
       updated_at = NOW()
 WHERE charge_point_id = (
       SELECT id FROM charge_points WHERE cp_id = '<CP_TARGET>'
   );
```

`charge_point_credentials` is keyed by `charge_point_id` (FK to `charge_points.id`), not by `cp_id` directly — hence the subquery.

**Verify the update applied:**

```sql
SELECT cp.cp_id, cred.updated_at
  FROM charge_points cp
  JOIN charge_point_credentials cred ON cred.charge_point_id = cp.id
 WHERE cp.cp_id = '<CP_TARGET>';
```

The `updated_at` should be within the last few seconds.

### What if the charger has no credential row?

Two cases:

- **Strict mode** (`ws_basic_auth_required=True`, the production posture): no credential row → reconnect denied automatically. Step 1 is a no-op; skip to step 2 to force the reconnect cycle.
- **Permissive mode** (dev / fleet-migration): no credential row → reconnect *succeeds*. To deny, INSERT a row with the rotated hash before doing step 2:

  ```sql
  INSERT INTO charge_point_credentials (charge_point_id, password_hash, created_at, updated_at)
  SELECT id, '<new-bcrypt-hash>', NOW(), NOW()
    FROM charge_points WHERE cp_id = '<CP_TARGET>';
  ```

  In permissive mode, **never DELETE** the row to deny the charger — that opens the gate, doesn't close it (see [`_basic_auth.py`](../src/eveys_ocpp/transport/_basic_auth.py)).

### When step 1 doesn't stick

Symptom: rotation succeeds, but the charger is still authenticating after step 2's reset. Root causes, in probability order:

1. **Wrong `cp_id`.** The runbook caller targeted a similar-looking ID. Verify against the charger's identity in `charge_points` and the running pod's logs.
2. **A second connection from a different gateway pod.** The rotation hits Postgres, but a sibling pod has the charger's old hash cached… except the gateway doesn't cache the hash. If you see this in production, it's a real bug — file an incident, fall back to step 3 (IP block) or step 4 (pod-kill) immediately.
3. **The Basic Auth check is disabled at the edge.** Verify `ws_basic_auth_required=True` is set in the running gateway: `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED` env var, or `gateway.basicAuth.required` in Helm values. If it's `False` AND the charger has no credential row, rotation is a no-op (per § "What if the charger has no credential row?").

## Step 2 — Force the reconnect with `Reset.Hard`

The charger's existing WebSocket survives step 1 because Basic Auth is upgrade-only. To force the reconnect cycle:

```bash
TOKEN=$(make get-token)   # or pull from your secrets manager
curl -fsS -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"type": "Hard"}' \
  https://<gateway>/api/v1/charge-points/<CP_TARGET>/commands/reset
```

Successful response (charger acknowledged the command):

```json
{ "status": "Accepted", "request_id": "<uuid>" }
```

OCPP 1.6 says the charger MUST drop and reconnect on `Reset.Hard`. On reconnect, it presents its old credentials, Basic Auth rejects with 401, the OCPP handler stack never sees the connection. Charger is now off.

If the charger answers `Rejected`, it's refusing the command — fall through to step 3 or 4.

## Step 3 (optional) — Network-edge IP block

Out of gateway scope; this is operator infrastructure (firewall, security group, Envoy `direct_response` rule). Use when:

- The charger is suspected malicious — credential rotation can be re-stolen, IP-block can't.
- The charger is flooding a shared resource and you need to defend the rest of the fleet from a misbehaving NAT.

The exact command depends on your edge setup. For Envoy-fronted production, an `direct_response` filter conditioned on the source IP is the right shape; coordinate with platform.

## Step 4 (last resort) — Pod-kill

The charger is on one specific gateway pod (per the consistent-hash routing in [ADR-0007](./adr/0007-envoy-as-the-load-balancer.md)). Kill that pod:

```bash
# Find the pod hosting the charger:
kubectl exec -n eveys-ocpp deploy/eveys-ocpp -- \
  redis-cli -u $REDIS_URL GET "cp:online:<CP_TARGET>"
# → returns the pod_id, e.g. "ocpp-gw-7b3fc9d-x4z8q"

kubectl delete pod -n eveys-ocpp ocpp-gw-7b3fc9d-x4z8q
```

The charger drops with the pod and tries to reconnect. Consistent-hash on `:path` (cp_id) routes it to a sibling pod; Basic Auth on the sibling rejects per step 1.

If step 1 wasn't done first, this is just churn — the charger gets back in on the sibling. Always pair pod-kill with credential rotation.

---

## Post-action verification

Within 30 s of the disconnect, confirm:

1. **Charger is offline in the registry**:

    ```bash
    redis-cli -u $REDIS_URL GET "cp:online:<CP_TARGET>"
    # → (nil)
    ```

2. **The gateway logged the reject** (the per-charger Basic Auth metric increments):

    ```promql
    eveys_ocpp_ws_basic_auth_total{outcome="bad_password"}
    ```

    A new sample point on this counter timestamped within the last
    minute is the evidence. (Or `outcome="no_credential"` in strict
    mode if you went the row-delete route.)

3. **No active transaction is stuck**:

    ```sql
    SELECT transaction_id, started_reported_at, stopped_reported_at
      FROM transactions
     WHERE cp_id = '<CP_TARGET>'
       AND stopped_reported_at IS NULL;
    ```

    An open row here means the disconnect happened mid-transaction.
    The session will be reconciled by the StopTransaction-replay
    path on whatever the next reconnect is — but if the charger is
    gone for good (decommissioned, stolen, vendor-bricked), the
    session needs manual close. Out of scope for the rollback
    itself; track separately.

## Reversing the rollback

To re-admit the charger:

```sql
UPDATE charge_point_credentials
   SET password_hash = '<original-hash>',
       updated_at = NOW()
 WHERE charge_point_id = (
       SELECT id FROM charge_points WHERE cp_id = '<CP_TARGET>'
   );
```

You needed the original hash before rotation — keep a backup of the row's `password_hash` value before step 1b, in your incident log. If you didn't, the charger needs a fresh password (and the operator on the charger side needs to re-provision it via the vendor's console).

## Drill — the "< 2 min" target

Phase 6's gate is "drill performed and timed":

1. Pick a non-production-traffic charger in staging.
2. Start a stopwatch on the decision.
3. Execute steps 1 and 2.
4. Verify post-action items 1–3 above.
5. Stop the stopwatch when the registry GET returns `(nil)`.

Pass: under 2 minutes wall-clock. Document the timing in the
Phase 6 postmortem.

A drill that takes longer than 2 minutes points at one of:

- The operator didn't have the bcrypt-generation tool ready (pre-bake the command in your secrets manager / ops repo).
- The Postgres connection setup took 30+ s (operator should have a long-lived `psql` session against the prod cluster, not a fresh `kubectl exec` per command).
- The charger took longer than 30 s to acknowledge `Reset.Hard` (vendor-specific; document in the postmortem so the next drill expects it).

## What this runbook is **not**

- **Not a graceful session end.** If you want to end an in-progress charging session politely (let the customer's car finish charging, then disconnect), use [`POST .../commands/remote-stop`](./integration/02-gateway-rest-api.md#post-apiv1charge-pointscp_idcommandsremote-stop) and don't run this runbook.
- **Not a way to expire a user account.** The gateway's authn is per-charger (via `charge_point_credentials`), not per-user. Operator account management is platform infra; out of scope.
- **Not a substitute for incident response.** If the disconnect is part of a wider incident (multiple chargers misbehaving, suspected platform-wide breach), page on-call first; this runbook is a tool the on-call playbook can refer back to.
- **Not a credential-rotation policy.** Periodic password rotation across the fleet is a different process (Helm-orchestrated; coordinate with vendor management). This runbook is the *unscheduled, individual* rotation.
