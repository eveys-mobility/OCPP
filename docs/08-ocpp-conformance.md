# 08 — OCPP conformance matrix (CSMS, OCPP 1.6)

> The audit-grade record of how `eveys/ocpp` (a **CSMS**, per [ADR-0005](./adr/0005-certification-target.md)) implements OCPP 1.6, **keyed by Appendix C test-case IDs** from the OCA OCPP 1.6 Certification Procedure. This is the artifact OCTT examiners, vendors, and internal QA all read.
>
> The cert-readiness *playbook* is in [`09-certification-readiness.md`](./09-certification-readiness.md). This doc is the *per-test-case* tracking matrix.

## Status legend

| Symbol | Meaning |
|---|---|
| ✅ | Implemented, unit-tested, OCTT subset green in CI for this TC, and reviewed against the OCA spec |
| 🟡 | Implemented and unit-tested, but **OCTT not yet run** OR **spec-citation pending**. Treated as **non-certifiable** |
| ⏳ | Not yet implemented; planned for the noted task |
| ❌ | Out of scope (with a reason) |

> **Today, every implemented row is 🟡.** No row promotes to ✅ until OCTT runs against it (which requires OCA membership — see Stream 1 in [`09-certification-readiness.md`](./09-certification-readiness.md)).

---

## Promotion process

A row moves from 🟡 to ✅ only when **all** of the following are true:

1. **OCTT subset run.** The relevant Appendix C test case has been run by OCTT against our CSMS (locally during dev, then in CI during W2+). Pass output is attached to the MR or stored in the QA shared drive.
2. **Spec section reviewed.** TL or QA cert lead has read the corresponding section of the OCPP 1.6 Edition 2 spec + Errata v4.0 against the handler. Cite the section in the row's Notes.
3. **Edge cases covered in unit tests.** Every status return code the spec allows is unit-tested, not just the happy path.
4. **Deviations declared.** Any intentional deviation from the spec is documented in the handler docstring AND in the Notes column AND has TL approval recorded in the MR.

**A 🟡 row may not be cited as "OCPP-conformant" in any external communication.** What we can say is in the [Defensible-claim template](#defensible-claim-template) at the bottom of this document.

---

## DUT type

`eveys/ocpp` is certified as a **CSMS** ("Central System" in 1.6 terminology), not as a Charging Station. Test cases marked "Charging Station only" in Appendix C are out of scope.

---

## Profiles in scope (per [ADR-0005](./adr/0005-certification-target.md))

- Core (mandatory)
- Smart Charging
- Advanced Security
- Reservations
- Local Authorization List Management
- Remote Trigger

---

## Conformance matrix — Core profile (CSMS-mandatory cases)

| TC ID | Scenario | Status | Implementation | Tests | Notes |
|---|---|---|---|---|---|
| TC_001 | Cold Boot Charge Point | 🟡 | [`handlers/v16/boot_notification.py`](../src/eveys_ocpp/handlers/v16/boot_notification.py) | [test_boot_notification.py](../tests/unit/handlers/v16/test_boot_notification.py) | Always Accepted; no charger blocklist; `interval` from settings. Replay-gated by Redis idempotency cache (E2-11, ADR-0017); emits `cp.boot` to Kafka (E2-8). |
| TC_003 | Regular Charging Session — Plugin First | 🟡 | [`handlers/v16/start_transaction.py`](../src/eveys_ocpp/handlers/v16/start_transaction.py) | [test_start_transaction.py](../tests/unit/handlers/v16/test_start_transaction.py) | Plugin-first flow accepted. |
| TC_004_1 | Identification First | 🟡 | `authorize.py` + `start_transaction.py` | [test_authorize.py](../tests/unit/handlers/v16/test_authorize.py), [test_start_transaction.py](../tests/unit/handlers/v16/test_start_transaction.py) | Auth-first flow accepted. |
| TC_004_2 | Identification First — ConnectionTimeOut | ⏳ | E2-5 | — | Requires RemoteStart timeout machinery. |
| TC_007 | Regular Start — Cached Id | ⏳ | E3-4 | — | Requires Redis Authorize cache. |
| TC_011_1 | Remote Start — Remote Start First | ⏳ | E2-5 | — | Requires gRPC RemoteStart command. |
| TC_011_2 | Remote Start — Time Out | ⏳ | E2-5 | — | |
| TC_012 | Remote Stop Charging Session | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`RemoteStop`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py), [test_local_smoke.py::test_grpc_remote_stop_dispatches_to_charger](../tests/e2e/test_local_smoke.py), [test_two_pod_dispatch.py](../tests/e2e/test_two_pod_dispatch.py) | Same-pod direct; off-pod via Redis pub/sub bus (E2-10, ADR-0016). |
| TC_013 | Hard Reset Without transaction | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`Reset`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | `RESET_TYPE_HARD` → OCPP `Hard`. |
| TC_014 | Soft Reset Without Transaction | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`Reset`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | `RESET_TYPE_SOFT` → OCPP `Soft`. |
| TC_017_1 | Unlock connector — Not fixed cable | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`UnlockConnector`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | `connector_id` required (>0). |
| TC_017_2 | Unlock connector — Fixed cable | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`UnlockConnector`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | OCPP `NotSupported` flows through to proto enum. |
| TC_021 | Change/set Configuration | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`ChangeConfiguration`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | All 4 OCPP statuses mapped (Accepted / Rejected / RebootRequired / NotSupported). Empty `key` → `INVALID_ARGUMENT`. |
| TC_023_1 | Authorize invalid | 🟡 | [`handlers/v16/authorize.py`](../src/eveys_ocpp/handlers/v16/authorize.py) | [test_authorize.py](../tests/unit/handlers/v16/test_authorize.py) | **Mock policy** — `INVALID*` → Invalid. Real auth-service in E3-3. |
| TC_023_2 | Authorize expired | ⏳ | E3-3 | — | Mock today never returns Expired; real auth-service must. |
| TC_023_3 | Authorize blocked | ⏳ | E3-3 | — | Mock today never returns Blocked. |
| TC_024 | Start Charging Session — Lock Failure | ⏳ | E2-1 | — | Charger reports lock failure; CSMS records. |
| TC_026 | Remote Start Charging Session — Rejected | ⏳ | E2-5 | — | |
| TC_028 | Remote Stop Transaction — Rejected | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`RemoteStop`) | [test_grpc_server.py::test_remote_stop_rejected](../tests/unit/transport/test_grpc_server.py) | OCPP `Rejected` → `REMOTE_STOP_STATUS_REJECTED`. |
| TC_030 | Unlock Connector — Unlock Failure | ⏳ | E2-1 | — | |
| TC_031 | Unlock Connector — Unknown Connector | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`UnlockConnector`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | `OCPP `UnlockFailed` for unknown connector flows through; `connector_id<=0` rejected at boundary. |
| TC_032_1 | Power failure — stop transactions before going down | 🟡 (partial) | [`handlers/v16/stop_transaction.py`](../src/eveys_ocpp/handlers/v16/stop_transaction.py) | [test_stop_transaction.py](../tests/unit/handlers/v16/test_stop_transaction.py) | StopTransaction handler accepts post-power-loss messages. Two-layer dedup on replay (E2-11, ADR-0017): Redis idempotency cache `(cp_id, message_id)` for the hot path + Postgres natural-key (`cp_id, transaction_id, meter_stop`) as defense in depth. |
| TC_040_1 | Configuration keys — NotSupported | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`ChangeConfiguration`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | OCPP `NotSupported` → `CHANGE_CONFIGURATION_STATUS_NOT_SUPPORTED`. |
| TC_040_2 | Configuration Keys — Invalid value | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`ChangeConfiguration`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | OCPP `Rejected` → `CHANGE_CONFIGURATION_STATUS_REJECTED`. |
| TC_045_1 | Get Diagnostics | ⏳ | E2-1 | — | Includes interop check on FTP. |
| TC_045_2 | Get Diagnostics — Upload Failed | ⏳ | E2-1 | — | |
| TC_054 | Trigger Message | 🟡 | [`transport/grpc_server.py`](../src/eveys_ocpp/transport/grpc_server.py) (`TriggerMessage`) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) | All 6 OCPP 1.6 §6.51 message kinds mapped. `NotImplemented` flows through. UNSPECIFIED at boundary → `INVALID_ARGUMENT`. |
| TC_062 | Data Transfer to a Charge Point | ⏳ | E2-1 | — | Reply UnknownVendorId is acceptable. |
| TC_064 | Data Transfer to a Central System | ⏳ | E2-1 | — | We accept and log; rejecting is also acceptable. |
| TC_073 | Update Charge Point Password | ⏳ | Phase 5 | — | Security extension. |
| TC_075_1 | Install ManufacturerRootCertificate | ⏳ | Phase 5 | — | |
| TC_075_2 | Install CentralSystemRootCertificate | ⏳ | Phase 5 | — | |
| TC_076 | Delete a specific certificate | ⏳ | Phase 5 | — | |
| TC_078 | Invalid CentralSystemCertificate Security Event | ⏳ | Phase 5 | — | |
| TC_079 | Get Security Log | ⏳ | Phase 5 | — | |
| TC_080 | Secure Firmware Update | ⏳ | Phase 5 | — | |
| TC_081 | Secure Firmware Update — Invalid Signature | ⏳ | Phase 5 | — | |
| TC_085 | Basic Authentication | ⏳ | Phase 5 | — | E5-6. |
| TC_086 | TLS server-side certificate | ⏳ | Phase 5 | — | |
| TC_088 | WebSocket Subprotocol negotiation | 🟡 | [`transport/ws_server.py`](../src/eveys_ocpp/transport/ws_server.py) | — (covered by simulator e2e) | Subprotocol `ocpp1.6` enforced. |

### Core profile — handlers shipped in W1 not on the mandatory CSMS list above

These are charger-initiated actions the CSMS must respond to. Appendix C tests them indirectly via the scenarios above; we still ship and unit-test them.

| Handler | Tests | Status | Notes |
|---|---|---|---|
| Heartbeat | [test_heartbeat.py](../tests/unit/handlers/v16/test_heartbeat.py) | 🟡 | Refresh `last_heartbeat_at`; return server UTC; refresh Redis online TTL. |
| StatusNotification | [test_status_notification.py](../tests/unit/handlers/v16/test_status_notification.py) | 🟡 | `last_status` row in Postgres + `cp.status` event published to Kafka per state transition (E2-8). Per-state history reaches ClickHouse via E2-14. |
| MeterValues (TC_070 sampled / TC_071 clock-aligned) | [test_meter_values.py](../tests/unit/handlers/v16/test_meter_values.py), [test_kafka_to_clickhouse.py](../tests/e2e/test_kafka_to_clickhouse.py) | 🟡 | Forwards to Kafka topic `cp.meter` (CpMeter envelope). Per-sample sanity check at 100 MWh (AGENTS rule 6). Postgres never sees MeterValues (AGENTS rule 4 + ADR-0004); ClickHouse `cp_meter` table consumes the topic via the sidecar ingestor (E2-14, ADR-0020). Producer durability and reconnect tuning landed via E2-7 (`acks=all` + idempotent producer + 30s request timeout + 5ms linger; ADR-0019). |
| DataTransfer (charger-initiated) | [test_data_transfer.py](../tests/unit/handlers/v16/test_data_transfer.py) | 🟡 | Vendor-extension envelope; default response is `UnknownVendorId` per OCPP spec. Operators wire vendor handlers explicitly when needed (none today). E2-1A. |
| DataTransfer (CSMS-initiated, gRPC) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) `test_data_transfer_*`, [test_two_pod_dispatch.py](../tests/e2e/test_two_pod_dispatch.py) `test_data_transfer_routes_across_two_pods` | 🟡 | Forwards `vendor_id` / `message_id` / `data` to the charger via the standard `_dispatch_ocpp_call` path. Empty `vendor_id` rejected at gateway boundary as `INVALID_ARGUMENT`. Cross-pod tested via the command bus (E2-10), including the optional `data` reply payload. E2-1A. |
| GetConfiguration (CSMS-initiated, gRPC) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) `test_get_configuration_*`, [test_two_pod_dispatch.py](../tests/e2e/test_two_pod_dispatch.py) `test_get_configuration_routes_across_two_pods` | 🟡 | Empty `keys` request forwarded as `None` (= "return all" per spec). Translates the OCPP dict-based response to typed `ConfigurationKey` proto messages. Missing `value` field coerced to empty string. Cross-pod tested via the command bus (E2-10) including the list-of-dicts response payload. E2-1A. |
| ClearCache (CSMS-initiated, gRPC) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) `test_clear_cache_*`, [test_two_pod_dispatch.py](../tests/e2e/test_two_pod_dispatch.py) `test_clear_cache_routes_across_two_pods` | 🟡 | Wipes the charger's local Authorize cache. Trivial wrapper; returns Accepted/Rejected. Cross-pod tested via the command bus (E2-10). E2-1A. |

---

## Conformance matrix — Smart Charging profile (CSMS, in scope)

| TC ID | Scenario | Status | Implementation |
|---|---|---|---|
| TC_056 | Central Smart Charging — TxDefaultProfile | 🟡 E2-1E | gRPC `SetChargingProfile` with `purpose=TxDefaultProfile`. Mirror in `charging_profiles` (Alembic `0005`); charger-side resolver per ADR-0022. e2e: `test_set_charging_profile_persists_mirror`. |
| TC_057 | Central Smart Charging — TxProfile | 🟡 E2-1E | gRPC `SetChargingProfile` with `purpose=TxProfile` and `transaction_id` populated. |
| TC_058_1 | No ongoing transaction | 🟡 E2-1E | Charger validates and reports `Rejected`; gateway translates and skips persistence. |
| TC_058_2 | Wrong transactionId | 🟡 E2-1E | Same `Rejected` path. |
| TC_059 | Remote Start Transaction with Charging Profile | 🟡 E2-5 + E2-1E | `RemoteStartRequest.charging_profile` carries the embedded `ChargingProfile` message; the proto was extended in E2-1E with the full field set. |
| TC_060 | Remote Start with Charging Profile — Rejected | 🟡 E2-5 + E2-1E | Same code path; charger Rejects → proto `REMOTE_START_STATUS_REJECTED`. |
| TC_066 | Get Composite Schedule | 🟡 E2-1E | gRPC `GetCompositeSchedule` round-trips to charger; charger-side resolver per ADR-0022. Translator handles per-period list, optional `start_schedule`, charging-rate-unit translation both directions. |
| TC_067 | Clear Charging Profile | 🟡 E2-1E | gRPC `ClearChargingProfile` with optional filters; on charger Accepted, mirror flips matching rows to `Cleared` (status flip, not deletion — audit trail preserved). e2e: `test_clear_charging_profile_marks_cleared`. |
| TC_082 | TxDefaultProfile with ongoing transaction | 🟡 E2-1E | Same `SetChargingProfile` code path; charger validates the ongoing-transaction interaction per spec. |

---

## Conformance matrix — Advanced Security profile (CSMS, in scope)

| TC ID | Scenario | Status | Implementation |
|---|---|---|---|
| TC_074 | Update Charge Point Certificate by request of Central System | ⏳ Phase 5 | mTLS work (E5-5) |
| TC_077 | Invalid ChargePointCertificate Security Event | ⏳ Phase 5 | |
| TC_087 | TLS — Client-side certificate — valid certificate | ⏳ Phase 5 | |

---

## Conformance matrix — Reservations profile (CSMS, in scope)

| TC ID | Scenario | Status | Implementation |
|---|---|---|---|
| TC_046 | Reservation of a Connector — Transaction | 🟡 E2-1C | gRPC `ReserveNow(connector_id>0)` flips a `Pending` row to `Active` on charger Accepted; consumed when charger sees a matching `StartTransaction` (charger-side enforcement; ADR-0021). e2e: `test_reserve_now_full_lifecycle`. |
| TC_047 | Reservation of a Connector — Expire | 🟡 E2-1C | Charger enforces `expiry_date` locally (untrusted clock per AGENTS rule 7); gateway computes effective expiry at query time per ADR-0021 § "no scheduler". |
| TC_048_4 | Reservation of a Connector — Rejected | 🟡 E2-1C | Charger reply `Occupied`/`Faulted`/`Unavailable`/`Rejected` translates to the matching proto enum and the Pending row is deleted (no orphan rows on refusal). e2e: `test_reserve_now_charger_occupied_drops_pending_row`. |
| TC_049 | Reservation of a Charge Point — Transaction | 🟡 E2-1C | `connector_id=0` reserves the whole charger per OCPP 1.6 spec. Same code path as TC_046. |
| TC_051 | Cancel Reservation | 🟡 E2-1C | gRPC `CancelReservation(reservation_id)` forwards to charger; on Accepted flips mirror to `Cancelled`; on Rejected leaves the row alone (charger's view wins). e2e covered in `test_reserve_now_full_lifecycle`. ADR-0021. |

---

## Conformance matrix — Local Authorization List profile (CSMS, in scope)

| TC ID | Scenario | Status | Implementation |
|---|---|---|---|
| TC_042_2 | Get Local List Version (empty) | 🟡 E2-1B | gRPC `GetLocalListVersion` round-trips through OCPP; charger is the source of truth (gateway-side mirror is for operator queries, not this RPC). e2e: `test_local_auth_list_get_version_reads_from_charger`. |
| TC_043_3 | Send Local Authorization List — Failed | 🟡 E2-1B | gRPC `SendLocalList`; charger-reported `Failed`/`NotSupported`/`VersionMismatch` translate to the matching proto enum and the gateway-side mirror is **not** updated (charger is source of truth). |
| TC_043_4 | Send Local Authorization List — Full | 🟡 E2-1B | `update_type=LOCAL_AUTH_LIST_UPDATE_TYPE_FULL` clears `local_auth_list_entries` and writes the new list on charger Accepted; bumps `local_auth_lists.list_version`. e2e: `test_local_auth_list_full_replace_persists_mirror`. |
| TC_043_5 | Send Local Authorization List — Differential | 🟡 E2-1B | Per-tag upsert (entry has `id_tag_info`) or delete (entry omits it) on charger Accepted, no full rewrite. Unit-tested for routing + boundary validation; e2e for the Full path; Differential coverage at e2e level deferred to a follow-up if/when an operator workflow needs it. |

---

## Conformance matrix — Remote Trigger profile (CSMS, in scope)

| TC ID | Scenario | Status |
|---|---|---|
| TC_054 | Trigger Message | 🟡 E2-6 — six message kinds covered; promotion to ✅ blocked on OCTT (C-1a). |

---

## Conformance matrix — FirmwareManagement profile (CSMS, in scope)

| Handler / RPC | Tests | Status | Notes |
|---|---|---|---|
| GetDiagnostics (CSMS-initiated, gRPC) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) `test_get_diagnostics_*`, [test_local_smoke.py](../tests/e2e/test_local_smoke.py) `test_diagnostics_get_then_status_notification` | 🟡 E2-1F | Forwards `location` (URL) + optional retries / time-window to charger; charger returns optional `file_name`. Inbound DiagnosticsStatusNotification populates `charge_points.last_diagnostics_status` for ops queries. |
| UpdateFirmware (CSMS-initiated, gRPC) | [test_grpc_server.py](../tests/unit/transport/test_grpc_server.py) `test_update_firmware_*`, [test_local_smoke.py](../tests/e2e/test_local_smoke.py) `test_firmware_update_then_status_notifications` | 🟡 E2-1F | OCPP UpdateFirmware.conf is empty per spec; gateway response carries no fields. Status arrives via inbound FirmwareStatusNotification. |
| DiagnosticsStatusNotification (charger-initiated) | [test_diagnostics_status_notification.py](../tests/unit/handlers/v16/test_diagnostics_status_notification.py) | 🟡 E2-1F | Latest-wins update of `charge_points.last_diagnostics_status` (Idle / Uploading / Uploaded / UploadFailed). Empty conf reply per spec. |
| FirmwareStatusNotification (charger-initiated) | [test_firmware_status_notification.py](../tests/unit/handlers/v16/test_firmware_status_notification.py) | 🟡 E2-1F | Latest-wins update of `charge_points.last_firmware_status`. Persists whatever string the charger reports — column width tolerates Phase-5 Security-profile additions (e.g. `SignatureVerified`, `InvalidSignature`) without schema change. |

---

## Schemas

The OCA-published JSON Schemas are bundled inside the `mobilityhouse/ocpp` PyPI package and are loaded by the library at every message validation point. We do **NOT** vendor them into our repo.

To inspect locally:

```bash
ls .venv/lib/python3.13/site-packages/ocpp/v16/schemas/
```

The library version is pinned in `pyproject.toml` (`ocpp>=2.1,<2.2`) so schema behavior cannot drift unintentionally between MRs. Bumping the pin requires re-running every promoted ✅ row's OCTT test in CI.

> **AGENTS hard rule 2 restated:** JSON Schemas are authoritative. Do not edit dataclasses without consulting the schema. Do not disable validation under load.

---

## Defensible-claim template

External communication (operator decks, RFP responses, marketing) about the CSMS conformance status:

> *"`eveys/ocpp` is built on the OCA-recommended `mobilityhouse/ocpp` library, which bundles and validates against the official OCA JSON Schemas (the wire format is therefore standard-compliant by construction). The CSMS is engineered against the OCPP 1.6 Certification Procedure (OCA, 2023) and is on track for OCA OCPP 1.6 certification covering the Core, Smart Charging, Advanced Security, Reservations, Local Authorization List, and Remote Trigger profiles. The cert program is detailed in [`docs/09-certification-readiness.md`](./09-certification-readiness.md); per-test-case status is tracked in this matrix. Final cert is contingent on (a) OCA membership and OCTT access, (b) lab engagement scheduled for W8."*

This is the strongest claim that holds today. **Anything stronger overstates.** In particular:

- ❌ "OCPP-certified" → No certificate has been issued.
- ❌ "OCTT-passing" → OCTT has not been run; OCA membership pending.
- ❌ "Conformant to OCPP 1.6" → No row promoted to ✅; promotion requires OCTT.

---

## How to update this document

1. **Adding a handler** → add row(s) keyed by Appendix C TC ID(s) in the right profile section, status starts at 🟡.
2. **Promoting a row to ✅** → only after the [Promotion process](#promotion-process) is complete; reference the MR + OCTT test artifact.
3. **Bumping the `ocpp` library minor version** → re-promote every previously ✅ row from 🟡 until the OCTT subset re-runs green.
4. **Schema drift detected by OCTT or by a real charger interop bug** → file a bug, mark the affected row 🟡, link the bug.

This document is part of **every** handler MR. A handler change without a matrix update is incomplete (AGENTS rule 8).

## References

- OCA — Open Charge Alliance: <https://www.openchargealliance.org/>
- OCPP 1.6 Certification Procedure (OCA, 2023) — see [`09-certification-readiness.md`](./09-certification-readiness.md). Copy held on shared drive.
- OCPP 1.6 Edition 2 specification — task C-1, on shared drive.
- `mobilityhouse/ocpp` library: <https://github.com/mobilityhouse/ocpp>
- ADR-0002 — Adopt mobilityhouse/ocpp: [`adr/0002-mobilityhouse-ocpp-library.md`](./adr/0002-mobilityhouse-ocpp-library.md).
- ADR-0005 — Certification target: [`adr/0005-certification-target.md`](./adr/0005-certification-target.md).
- [`docs/03-coding-standards.md`](./03-coding-standards.md) — project conventions, including the OCPP-specific hard rules.
