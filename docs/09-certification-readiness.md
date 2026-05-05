# 09 — OCPP 1.6 certification readiness

> The playbook for getting `eveys/ocpp` OCA-certified as an OCPP 1.6 CSMS. Maps every requirement from the OCA OCPP 1.6 Certification Procedure to a concrete owner, deliverable, and timeline. **Read alongside [`08-ocpp-conformance.md`](./08-ocpp-conformance.md)** — that doc is the per-handler matrix; this doc is the program-level readiness checklist.

## Scope decided in [ADR-0005](./adr/0005-certification-target.md)

| Decision | Value |
|---|---|
| DUT type | **CSMS** (pure central system; not also publishing a Charging Station Software Stack) |
| Profiles to certify | Core · Smart Charging · Advanced Security · Reservations · Local Authorization List · Remote Trigger |
| Lab engagement | After W8 hardening (task **P-7**) |
| Vendor Declaration of Conformance | Reserved for adding future product-family members; not for first-time cert |

---

## What "certified" actually requires

Per the OCA Certification Procedure, a CSMS becomes certified when **all** of the following are true:

1. **Conformance tests pass** — every mandatory test case in Appendix C (column "Conf. test for Central System" = `M`) and every applicable conditional test case (column = `C`, condition met) passes against OCTT in lab mode.
2. **Performance measurements are recorded** — every parameter the vendor declares in the PICS for performance is measured in lab and attached to the certificate. Values do **not** have to fall in a range; they are published informationally.
3. **PICS is filed** — three documents: PICS for OCPP 1.6 (functionalities), PICS for OCPP 1.6 Security, PICS for OCPP 1.6 Performance Measurement.
4. **A running CSMS is provided** to the lab — either a copy on a server they control, or accessible over the internet.
5. **Test laboratory engagement** — an OCA-designated lab runs OCTT against our CSMS; the cert is awarded by OCA after the lab reports a clean run.

**Crashes during a test → automatic re-test required.** Improper handling by the lab tester is excluded.

---

## Readiness streams

Cert readiness has four parallel streams. Each has an owner and gates the cert run.

### Stream 1 — OCA membership + OCTT access (project-management)

| Step | Owner | Status | Notes |
|---|---|---|---|
| Begin OCA membership process | **Manager + TL** | ⏳ Not started | OCA membership is the prerequisite for OCTT access. **This is a critical-path dependency** — no membership = no OCTT = no cert. |
| Receive OCTT 1.6 distribution from OCA | TL | ⏳ Pending membership | OCTT is distributed to members; we cannot run it locally before this. |
| Identify OCA-designated test laboratory | Manager | ⏳ Not started | Lab list is published by OCA. Begin scoping, pricing, and scheduling once membership is active. |
| Pre-book lab slot for W8 / W9 of the implementation plan | Manager | ⏳ Not started | Lab calendars run weeks ahead; book before W6 to reserve W8 slot. |

### Stream 2 — Conformance implementation (engineering)

| Step | Owner | Status | Notes |
|---|---|---|---|
| Implement Core profile actions | SB1, SB2 | 🟡 done (E2-1A) | All Core handlers shipped: 7 from W1 + `DataTransfer`/`GetConfiguration`/`ClearCache` from E2-1A. Promotion to ✅ blocked on OCTT (C-1a, deferred). |
| Implement Smart Charging profile actions | SB1, SB2 | ⏳ E2-1E | TC_056..TC_072 in Appendix C. Charging profiles, schedules, stacking. Own ADR pending. |
| Implement Advanced Security profile actions | SRE, SB1 | ⏳ Phase 5 | TLS 1.2+ with client-side certs. Already on hardening roadmap (E5-5, E5-6). |
| Implement Reservations profile actions | SB1, SB2 | 🟡 done (E2-1C) | TC_046..TC_053 in Appendix C. `reservations` table via Alembic `0003`; charger-side authority + gateway-side mirror per ADR-0021. Promotion to ✅ blocked on OCTT. |
| Implement Local Authorization List profile actions | SB1, SB2 | 🟡 done (E2-1B) | TC_042, TC_043, TC_008 in Appendix C. `local_auth_lists` + `local_auth_list_entries` tables via Alembic `0002`. Promotion to ✅ blocked on OCTT. |
| Implement Remote Trigger profile actions | SB1 | 🟡 done (E2-6) | TC_054, TC_055 in Appendix C. `TriggerMessage` shipped E2-6 covering all six message kinds. Promotion to ✅ blocked on OCTT. |
| Every handler MR cites Appendix C TC IDs in the conformance matrix | All engineers | 🟡 in progress | Required by AGENTS.md OCPP rule 8. |
| Every handler MR ships unit tests covering every status return code the spec allows | All engineers | 🟡 partial | Tightened in Phase 2 reviews. |

### Stream 3 — OCTT in CI (continuous conformance)

| Step | Owner | Status | Notes |
|---|---|---|---|
| Stand up OCTT against `make compose-up` + running CSMS | QA | ⏳ Pending OCTT access | Task **C-2**. First test case to run: `TC_001` (Cold Boot Charge Point). |
| Wire OCTT 1.6 Core subset into GitLab CI (non-blocking) | QA + SB2 | ⏳ Pending OCTT | Task **C-3**. Becomes blocking before W6. |
| Wire OCTT Smart Charging subset into CI | QA | ⏳ Pending OCTT + E2-1E | Once Smart Charging handlers (E2-1E) exist AND OCTT access lands. |
| Wire OCTT Reservations + LocalAuthList + RemoteTrigger subsets | QA | ⏳ Pending OCTT | Handlers shipped (E2-1B + E2-1C + E2-6); CI wiring blocked on OCTT access (task C-1a, deferred). |
| Wire OCTT Advanced Security subset | QA | ⏳ Phase 5 | After mTLS work (E5-5). |
| Promote handler rows in [`08-ocpp-conformance.md`](./08-ocpp-conformance.md) from 🟡 → ✅ as OCTT passes | TL | ⏳ Per handler | Per the four-step promotion process in that doc. |

### Stream 4 — PICS preparation (paperwork)

The PICS is the vendor's signed declaration of what the CSMS supports. Three PICS documents are needed; templates live in Appendix A of the Certification Procedure.

| PICS | What it declares | Owner | Status |
|---|---|---|---|
| PICS for OCPP 1.6 | Profiles supported, optional features (C-02..C-11, R-0..R-1, SC-1, LA-0, RT-0), additional CSMS questions (AQ-10) | TL | ⏳ Drafted in W6, frozen by W8 |
| PICS for OCPP 1.6 Security | Supported security profiles (1, 2, 3); cipher suites (CSMS must support all four mandatory); cert chain limits (`CertificateSignedMaxChainSize`, `CertificateStoreMaxLength`) | SRE | ⏳ Phase 5 |
| PICS for OCPP 1.6 Performance Measurement | Measured values for: OCPP triggered function timeout, OCPP response timeout, **Response time Authorize** (CSMS-specific), Transaction authorization time by RemoteStartTransaction, Transaction authorization end time by RemoteStopTransaction, communication technology used | SRE + QA | ⏳ Phase 4 (W6 load test produces values) |

**The PICS is filed pre-test and cannot be changed during a run.** Treat PICS freeze (W8) as a hard milestone.

---

## CSMS-side mandatory test cases (extracted from Appendix C)

Per Appendix C, the following CSMS test cases are mandatory (column "Conf. test for Central System" = `M`). This list scopes the engineering work — every handler / command path on this list must work with OCTT before the lab visit. Conditional cases (`C`) become mandatory if their condition is satisfied by our PICS declarations.

### Core profile (mandatory)

| TC ID | OCPP scenario | Implementation | Status |
|---|---|---|---|
| TC_001 | Cold Boot Charge Point | `handlers/v16/boot_notification.py` | 🟡 |
| TC_003 | Regular Charging Session — Plugin First | StartTransaction handler accepts plugin-first flow | 🟡 |
| TC_004_1 | Regular Charging Session — Identification First | StartTransaction handler accepts auth-first flow | 🟡 |
| TC_004_2 | Identification First — ConnectionTimeOut | RemoteStart timeout handling | ⏳ E2-5 |
| TC_007 | Regular Start Charging Session — Cached Id | Authorization cache | ⏳ E3-4 |
| TC_011_1 | Remote Start Charging Session — Remote Start First | gRPC `RemoteStart` → OCPP `RemoteStartTransaction` | ⏳ E2-5 |
| TC_011_2 | Remote Start Charging Session — Time Out | RemoteStart with timeout | ⏳ E2-5 |
| TC_012 | Remote Stop Charging Session | gRPC `RemoteStop` → OCPP `RemoteStopTransaction` | ⏳ E2-6 |
| TC_013 | Hard Reset Without transaction | gRPC `Reset(Hard)` | ⏳ E2-6 |
| TC_014 | Soft Reset Without Transaction | gRPC `Reset(Soft)` | ⏳ E2-6 |
| TC_017_1 / TC_017_2 | Unlock connector — no charging session running | gRPC `UnlockConnector` | ⏳ E2-6 |
| TC_021 | Change/set Configuration | gRPC `ChangeConfiguration` | ⏳ E2-6 |
| TC_023_1 / TC_023_2 / TC_023_3 | Start Charging Session — Authorize invalid / blocked / expired | Authorize policy from auth-service | ⏳ E3-3 |
| TC_024 | Start Charging Session — Lock Failure | Charger reports lock failure during start | ⏳ E2-1 |
| TC_026 | Remote Start Charging Session — Rejected | gRPC RemoteStart rejection path | ⏳ E2-5 |
| TC_028 | Remote Stop Transaction — Rejected | gRPC RemoteStop rejection path | ⏳ E2-6 |
| TC_030 | Unlock Connector — Unlock Failure | Charger reports unlock failure | ⏳ E2-1 |
| TC_031 | Unlock Connector — Unknown Connector | gRPC validation rejects unknown connector | ⏳ E2-6 |
| TC_032_1 | Power failure — configured to stop transaction(s) before going down | Charger sends StopTransaction; we accept | 🟡 (partial — stop_transaction.py) |
| TC_040_1 / TC_040_2 | Configuration keys — NotSupported / Invalid value | gRPC ChangeConfiguration error responses | ⏳ E2-6 |
| TC_045_1 / TC_045_2 | Get Diagnostics / Upload Failed | gRPC `GetDiagnostics` | ⏳ E2-1 |
| TC_054 | Trigger Message | gRPC `TriggerMessage` | ⏳ E2-6 |
| TC_062 / TC_064 | DataTransfer to/from CSMS | DataTransfer handler (reply UnknownVendorId) | ⏳ E2-1 |
| TC_073 / TC_075_1 / TC_075_2 / TC_076 | Update password / install / delete certificates | Security extension commands | ⏳ Phase 5 |
| TC_078 / TC_079 | Invalid CentralSystemCertificate Security Event / Get Security Log | Security event handling + log retrieval | ⏳ Phase 5 |
| TC_080 / TC_081 | Secure Firmware Update / Invalid Signature | Firmware update flow | ⏳ Phase 5 |
| TC_085 / TC_086 / TC_088 | Basic Auth / TLS server-side / WebSocket Subprotocol negotiation | Connection setup | 🟡 (partial — `transport/ws_server.py` does subprotocol; auth pending) |

### Smart Charging profile (mandatory if profile certified — we are)

| TC ID | OCPP scenario | Implementation | Status |
|---|---|---|---|
| TC_056 | Central Smart Charging — TxDefaultProfile | gRPC SetChargingProfile (default) | ⏳ Phase 2/3 |
| TC_057 | Central Smart Charging — TxProfile | gRPC SetChargingProfile (per-tx) | ⏳ Phase 2/3 |
| TC_058_1 / TC_058_2 | No ongoing transaction / Wrong transactionId | Validation + error responses | ⏳ Phase 2/3 |
| TC_066 | Get Composite Schedule | gRPC GetCompositeSchedule | ⏳ Phase 2/3 |
| TC_067 | Clear Charging Profile | gRPC ClearChargingProfile | ⏳ Phase 2/3 |
| TC_082 | TxDefaultProfile with ongoing transaction | Profile management mid-tx | ⏳ Phase 2/3 |
| TC_059 / TC_060 | Remote Start Transaction with Charging Profile / Rejected | RemoteStart payload extension | ⏳ Phase 2/3 |

### Reservations profile (we are certifying)

| TC ID | OCPP scenario | Status |
|---|---|---|
| TC_046 | Reservation of a Connector — Transaction | ⏳ Phase 2 |
| TC_047 | Reservation of a Connector — Expire | ⏳ Phase 2 |
| TC_048_4 | Reservation of a Connector — Rejected | ⏳ Phase 2 |
| TC_049 | Reservation of a Charge Point — Transaction | ⏳ Phase 2 |
| TC_051 | Cancel Reservation | ⏳ Phase 2 |

### Local Authorization List profile (we are certifying)

| TC ID | OCPP scenario | Status |
|---|---|---|
| TC_042_2 | Get Local List Version (empty) | ⏳ Phase 3 |
| TC_043_3 | Send Local Authorization List — Failed | ⏳ Phase 3 |
| TC_043_4 | Send Local Authorization List — Full | ⏳ Phase 3 |
| TC_043_5 | Send Local Authorization List — Differential | ⏳ Phase 3 |

### Remote Trigger profile (we are certifying)

| TC ID | OCPP scenario | Status |
|---|---|---|
| TC_054 | Trigger Message | ⏳ E2-6 (Phase 2) |

### Advanced Security profile (we are certifying)

| TC ID | OCPP scenario | Status |
|---|---|---|
| TC_074 | Update Charge Point Certificate by request of Central System | ⏳ Phase 5 |
| TC_077 | Invalid ChargePointCertificate Security Event | ⏳ Phase 5 |
| TC_087 | TLS — Client-side certificate — valid certificate | ⏳ Phase 5 |

> **Authoritative source is Appendix C** of the OCA OCPP 1.6 Certification Procedure. The list above is extracted from that appendix; if the appendix is updated by OCA, update this section to match.

---

## Performance measurement targets

Per Appendix A.2 of the Certification Procedure, the CSMS must declare and the lab must measure:

| Parameter | Unit | Notes for `eveys/ocpp` |
|---|---|---|
| OCPP response timeout | seconds | The longest we wait for an OCPP response message before declaring failure. Measured by OCTT logs. Project SLO target TBD; Phase 4 (E4-8). |
| **Response time Authorize** (CSMS-specific) | seconds | Time from receiving `Authorize.req` to sending `Authorize.conf`. **This becomes a production SLO** by virtue of being on the cert. Target: **P95 < 50 ms** (consistent with E3-4 cache requirement). |

The remaining Appendix A.2 parameters (`OCPP triggered function timeout`, `Transaction authorization time by RemoteStartTransaction`, `Transaction authorization end time by RemoteStopTransaction`) are Charging-Station-only — not applicable to our CSMS DUT.

**Network-connection requirements during measurement** (per §9.2.2): bandwidth ≥ 5 kB/s, latency ≤ 1000 ms. We meet these by construction in any reasonable lab setup.

---

## Pass criteria summary

A cert run passes when:

1. **All mandatory test cases pass** in OCTT. Mandatory = column "Conf. test for Central System" = `M`. Conditional = `C` *and* the condition is satisfied by our PICS.
2. **No DUT crash requires a reset.** §5.5: a crash during testing = automatic re-test.
3. **Performance parameters are measured and recorded.** Values themselves are informational — no min/max — but they must be measured.
4. **PICS is consistent with observed behavior.** If we declare "Reservations: Yes" but OCTT finds the reservation tests fail, the run fails.

---

## Re-test policy (what happens if we fail)

Per §5.5:

- Cert is awarded *per device + per firmware/OCPP-software-version*. Any change to either invalidates the cert and forces a full re-test.
- Re-tests are paid lab engagements. Budget and time loss for one re-test cycle: **assume 2 weeks calendar + lab fees**.
- Mitigation: OCTT-in-CI (Stream 3) catches the same things the lab will, before the lab visit. If CI is green for two consecutive weeks ahead of the lab, the re-test risk is small.

---

## Issue handling during the cert run (per §5.6)

Two categories:

| Issue type | Resolution path |
|---|---|
| Configuration / setup issues | Resolved live during the test, with help from a technical representative on our side. **If a non-OCPP configuration changes during testing, all certification tests start over.** |
| Bugs in software / hardware | Counted as failure → re-test required after fix. |

The TL or a designated SRE must be available on-call (synchronously) during the lab sitting.

---

## Cert-readiness exit gate (before booking the lab)

The lab visit is booked only when:

- ✅ OCA membership active; OCTT in our possession (Stream 1)
- ✅ All six profiles' handlers shipped + unit tests + integration tests (Stream 2)
- ✅ OCTT 1.6 full Core + Smart Charging + Advanced Security + Reservations + LocalAuthList + RemoteTrigger subsets green in CI for **two consecutive weeks** (Stream 3)
- ✅ Three PICS documents drafted (functional, security, performance), reviewed by TL, signed off (Stream 4)
- ✅ A running staging-equivalent CSMS instance has been demonstrated end-to-end with the simulator and is ready to be exposed to the lab
- ✅ Performance measurements taken in W6 load test, values recorded in the performance PICS

This is the W8 / start-of-W9 milestone. Anything missing → defer the lab.

---

## What we cannot honestly claim today

- ❌ "OCPP 1.6 certified" — we have no certificate, no OCA membership, no OCTT runs
- ❌ "OCTT-passable" — OCTT has not been run
- ❌ "Conformant" — no row in the conformance matrix is at ✅

What we can defensibly claim is in the corresponding section of [`08-ocpp-conformance.md`](./08-ocpp-conformance.md).

---

## References

- [ADR-0005 — Certification target](./adr/0005-certification-target.md) — the decisions this doc operationalizes
- [`08-ocpp-conformance.md`](./08-ocpp-conformance.md) — per-handler conformance matrix
- [`02-tasks.md`](./02-tasks.md) — task IDs (C-1..C-5, P-7) referenced above
- OCA OCPP 1.6 Certification Procedure — on project shared drive (copyright OCA, do not commit)
- OCA list of designated test laboratories — published on the OCA website
