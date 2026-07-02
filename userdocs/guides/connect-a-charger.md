# Connect a charger

**Audience.** A site engineer or integrator pointing an OCPP 1.6 charger at this gateway for the first time. The charger itself can be a real piece of hardware on a workbench or a simulator on the same workstation.

**What this answers.** What URL the charger uses, how Basic Auth works, what a successful boot looks like, and the most common failures with the fix for each.

> If you're brand new, read [`../02-quickstart.md`](../02-quickstart.md) first — it brings a simulator up against a local Compose stack in ten minutes. This page is the next layer: real chargers, real networks, real fault modes.

---

## 1. What the charger needs to know

Every OCPP 1.6 charger configuration screen asks the same three things:

| Field | Value | Example |
|---|---|---|
| **Server URL** | The base WebSocket URL the gateway is reachable at. | `wss://ocpp.example.com` (production) or `ws://10.0.0.5:19000` (dev) |
| **Charge Point ID** | A stable identifier you assign per charger. | `CP_ACME_BERLIN_42` |
| **Authorization key** (Basic Auth password) | The shared secret for this charger. | An opaque string you provisioned |

The final WebSocket URL the charger opens is `<server-url>/<cp_id>`. The charger sends `Authorization: Basic <base64(cp_id:password)>` on the WebSocket upgrade.

The WebSocket subprotocol the charger requests must be exactly `ocpp1.6` (or `ocpp2.0.1` for 2.0.1 chargers). The gateway rejects upgrades that don't negotiate one of those.

---

## 2. Provisioning the charger in the gateway

Two things have to happen on the gateway side before a charger can connect.

### 2.1 Pick the `cp_id`

It's a string. Pick a scheme that:

- **Is stable.** Once chosen for a physical charger, never reuse for another. The whole platform indexes on this.
- **Survives URLs.** Letters, digits, `_` and `-`. Avoid `/`, spaces, anything that needs escaping.
- **Tells you something at a glance.** `CP_SITE_42` beats `CP_42` beats an opaque UUID.

The same `cp_id` becomes the partition key on every Kafka event and the path parameter on every REST query about this charger.

### 2.2 Provision the credential

The gateway looks up the per-charger password in Postgres on every WebSocket upgrade. To add a charger:

```bash
# Generate a strong password
PASSWORD=$(openssl rand -base64 32)

# Hash it (bcrypt) and store in the credentials table.
# Replace CP_ACME_42 with the cp_id you chose.
# Replace the DSN with your gateway's database.
psql "$EVEYS_OCPP_DB_URL" <<SQL
INSERT INTO charge_points (cp_id) VALUES ('CP_ACME_42')
  ON CONFLICT (cp_id) DO NOTHING;

INSERT INTO charge_point_credentials (charge_point_id, password_hash)
SELECT id, crypt('$PASSWORD', gen_salt('bf', 12))
FROM charge_points WHERE cp_id = 'CP_ACME_42'
ON CONFLICT (charge_point_id) DO UPDATE SET password_hash = EXCLUDED.password_hash;
SQL

# Hand $PASSWORD to whoever configures the charger.
```

> **About `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED`.** By default this is `false` — chargers without a credential row connect anyway. That's a migration shim for fleets onboarding to OCPP for the first time. Production should set it to `true` once every charger is provisioned, so an un-provisioned charger fails fast at the WebSocket upgrade instead of slipping through unauthenticated.

---

## 3. Pointing the charger at the gateway

This step lives on the charger's web interface, vendor app, or DIP-switch configuration — every OEM has a different mechanism. The values are the same:

- **Server URL.** Production: `wss://<your-edge-hostname>` (TLS via Envoy). Local dev: `ws://<gateway-host>:19000`.
- **Charge Point ID.** The `cp_id` you provisioned in §2.
- **Authorization key.** The password from §2.

Once configured, the charger opens the WebSocket within a few seconds (typically on reboot or on hitting "Apply" on the config page).

---

## 4. What a healthy boot looks like

Watch the gateway's logs. A successful first connection produces a sequence:

```
ws.upgrade.accepted          cp_id=CP_ACME_42 subprotocol=ocpp1.6
boot_notification            cp_id=CP_ACME_42 vendor=ACME model=Charger-V2 firmware=1.4.2 status=Accepted
status_notification          cp_id=CP_ACME_42 connector_id=1 status=Available error_code=NoError
heartbeat                    cp_id=CP_ACME_42
heartbeat                    cp_id=CP_ACME_42      # every interval seconds...
```

On Kafka you'll see one envelope on `cp.connected`, one on `cp.boot`, one on `cp.status`, and a stream of heartbeats absorbed by the registry (heartbeats don't publish events by default — they're noise at fleet scale).

Confirm from REST:

```bash
curl -s http://<gateway>/api/v1/charge-points/CP_ACME_42 \
  -H "Authorization: Bearer $TOKEN" \
  | python3 -m json.tool
```

`last_boot_at`, `last_heartbeat_at`, and the `connectors` array all reflect what just happened.

---

## 5. When it doesn't work — the failure catalogue

These are the failures you'll see in roughly the order they occur as a charger boots.

### 5.1 Connection refused / no response

The charger logs a TCP-level failure trying to open the socket.

- **Cause:** the gateway isn't reachable on the configured host:port.
- **Check:** from the charger's network, `nc -vz <host> <port>`. If that fails, the charger can't reach the gateway. Firewall, NAT, wrong port.
- **Fix:** confirm the WebSocket port is exposed (`19000` in Compose; `443` behind Envoy in production).

### 5.2 TLS handshake fails

Charger logs an SSL error.

- **Cause:** the charger doesn't trust your TLS certificate, or the certificate's SAN doesn't include the hostname the charger is dialling.
- **Check:** `openssl s_client -connect <host>:443 -servername <host>` from a machine on the charger's network.
- **Fix:** install your CA on the charger (most chargers have a "trusted roots" upload), or re-issue the cert with the right SAN.

### 5.3 HTTP 401 / 403 on the WebSocket upgrade

Charger logs an authentication error.

- **Cause:** the `Authorization` header didn't match what's in Postgres.
- **Check:** gateway log line `ws.basic_auth.failed cp_id=CP_X reason=...`.
- **Fix:** re-provision the password (§2.2). If the gateway logs `reason=no_credential_row` and `EVEYS_OCPP_WS_BASIC_AUTH_REQUIRED=true`, you need to insert the row.

### 5.4 HTTP 426 / "Upgrade required" / subprotocol error

- **Cause:** the charger isn't requesting `ocpp1.6` (or `ocpp2.0.1`) as the WebSocket subprotocol.
- **Check:** gateway log line `ws.upgrade.rejected reason=no_acceptable_subprotocol`.
- **Fix:** firmware config issue on the charger; some OEMs let you override the subprotocol string. The exact value the charger sends should be `ocpp1.6`.

### 5.5 WebSocket connects but `BootNotification` is `Rejected`

The connection opened, but the gateway answered the very first OCPP CALL with `status: Rejected`.

- **Cause:** the gateway's policy for new chargers is "Rejected" (rare; the default is "Accepted"). Or your backend's `charge-points/register` endpoint returned a negative answer.
- **Check:** gateway log line `boot_notification ... status=Rejected reason=...`.
- **Fix:** review the backend's registration response — see how it's expected to behave in the security model concept page.

### 5.6 `BootNotification` succeeds, but no `Heartbeat`s

- **Cause:** the charger interprets the boot reply's `interval` as `0` and so never heartbeats. Some older firmwares mishandle that.
- **Check:** `BootNotification` reply payload in gateway logs — `interval` should be a positive integer (typically 30–300).
- **Fix:** raise `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` to a sane default (e.g. 60) so even buggy firmwares hold the socket open with traffic.

### 5.7 Heartbeat works but no `StatusNotification`

- **Cause:** the charger thinks every connector is faulted or unavailable and isn't reporting state changes.
- **Check:** charger's own diagnostic UI — most OEMs show connector state directly. Also: dispatch a `TriggerMessage` of type `StatusNotification` from the gateway:

  ```bash
  curl -s -X POST http://<gateway>/api/v1/charge-points/CP_ACME_42/commands/trigger-message \
    -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
    -d '{"requested_message":"StatusNotification","connector_id":1}'
  ```
- **Fix:** charger-side. If `TriggerMessage` returns `NotImplemented`, the OEM doesn't support it (uncommon).

### 5.8 Socket disconnects every few minutes

- **Cause:** a network device between the charger and the gateway is killing idle TCP connections. Heartbeats every 30–300 s normally prevent this, but some routers are aggressive.
- **Fix:** lower `EVEYS_OCPP_HEARTBEAT_INTERVAL_SECONDS` (the gateway returns this as the boot reply's `interval`). Don't go below 10 s — you'll flood Kafka and Postgres without gaining anything.

### 5.9 Disconnects after a `RemoteStart` from the backend

- **Cause:** typically a charger-side firmware bug; the OCPP CALL exceeds whatever the charger expects.
- **Check:** gateway log around the RemoteStart, then the charger's own log.
- **Fix:** OEM-specific. File against the OEM with the exact request payload from the gateway log.

---

## 6. When you're stuck

The gateway emits a structured JSON log line per OCPP CALL. Filter by `cp_id` and you'll see exactly what crossed the wire in both directions:

```bash
docker logs eveys-ocpp 2>&1 | jq 'select(.cp_id == "CP_ACME_42")'
# or in k8s:
kubectl -n eveys-ocpp logs -l app=eveys-ocpp -f | jq 'select(.cp_id == "CP_ACME_42")'
```

If you can't reproduce the issue, the simulator can drive an arbitrary sequence of CALLs at the gateway from your workstation — useful for proving the gateway works in isolation when the charger keeps crashing. See [`../02-quickstart.md`](../02-quickstart.md) §4 for the one-liner.

---

## Where to go from here

- **Backend integration**: [`use-the-rest-api.md`](./use-the-rest-api.md), [`consume-events.md`](./consume-events.md).
- **Production**: [`deploy-to-production.md`](./deploy-to-production.md).
- **What's happening end-to-end**: [`../concepts/how-ocpp-flows-work.md`](../concepts/how-ocpp-flows-work.md).
