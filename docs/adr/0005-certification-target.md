# ADR-0005 — Certification target: OCPP 1.6 CSMS, all profiles

- **Status**: Accepted
- **Date**: 2026-04-29
- **Author**: Eveys engineering
- **Reviewers**: TBD

## Context

`eveys/ocpp` will be put forward for **OCA OCPP 1.6 certification**. The OCPP 1.6 Certification Procedure (OCA, Edition 2023) describes:

- **Two DUT types** can be certified: Charging Station, or Central System Management System (CSMS). They are tested with different OCTT API surfaces.
- **Three named profiles** that a certificate can carry: Core, Smart Charging, Advanced Security.
- **Optional profiles** declared in the PICS: Reservations, LocalAuthList, RemoteTrigger, plus security profile selections.
- **Two paths** to a "certified" outcome:
  1. **Full certificate** — tested by an OCA-designated test laboratory using OCTT in lab mode. Mandatory for first-time certification of a product family.
  2. **Vendor Declaration of Conformance** — OCTT in PICS mode, signed digitally. Not a full certificate. Useful for adding family members after the first lab-tested member exists.

We have to decide three things up front, because they cascade through the implementation plan: which DUT type we are, which profiles to certify, and how we engage the lab.

Constraints:

- We are not building charging-station hardware. Other vendors' chargers connect to us.
- A certificate is sold to operators as evidence of interoperability. The broader the profile coverage, the broader the operator pool.
- OCTT access requires OCA membership. We do not have OCA membership yet (project-management dependency).
- The implementation plan budgets W7–W8 for hardening and W9 for staging soak; lab engagement fits naturally at the end of W8.

## Decision

1. **DUT type: pure CSMS.** We certify only the central-system side. We do not also publish a reference Charging Station Software Stack.
2. **Profiles to certify:**
   - **Core** (mandatory by definition)
   - **Smart Charging**
   - **Advanced Security** (TLS 1.2+ with client-side certificates)
   - **Reservations**
   - **Local Authorization List Management**
   - **Remote Trigger**

   Every optional functionality declared in the OCA PICS template (Appendix A.1 of the Certification Procedure) that is technically applicable to a CSMS is in scope. We do **not** voluntarily narrow scope; if a profile is implementable on the CSMS side, we certify against it.

3. **Lab engagement: W8 of the main track**, aligned with task **P-7** in the parallel cert track of [`06-implementation-plan.md`](../06-implementation-plan.md). Earlier intake is rejected — engaging before Phase 2 is complete forces multiple paid lab runs.

4. **OCTT path:** OCTT in CI is task **C-3** (now expanded; see [`02-tasks.md`](../02-tasks.md)). Final cert run is by an OCA-designated test laboratory, not via Vendor Declaration. Vendor Declaration is reserved for adding future product-family members (e.g., a managed-cloud variant of the same CSMS).

## Alternatives considered

- **Pure CSMS, Core only** — rejected. A Core-only certificate excludes operators who require Smart Charging (load balancing) or Reservations. Pre-emptive narrowing trades cert breadth for ~2 weeks of engineering, which the parallel track already absorbs.
- **CSMS + Charging Station Software Stack reference implementation** — rejected. We are not in the charger-software business; we'd be certifying scope we don't own and don't operate, just to demonstrate it.
- **Vendor Declaration of Conformance instead of lab cert** — rejected for the first product. Vendor Declaration is expressly for *adding* products to a certified family, not for first-time cert of any product. It would also be marketed weaker by operators.
- **Engage lab at W3** — rejected. Lab runs are paid per attempt; failures during early dev would burn budget. Hardening (Phase 5) and staging soak (Phase 6) must come first.
- **Engage lab in W14+ (after rollout)** — rejected. Would slip the announce-able cert claim past production GA, undermining the value to operators.

## Consequences

### Positive

- Broadest credible cert claim: Core + Smart Charging + Advanced Security + Reservations + LocalAuthList + RemoteTrigger covers every operator scenario we know of.
- One-shot lab engagement keeps the cert budget bounded (single sitting, single re-test if needed).
- OCTT-in-CI from W2 onwards means failures surface continuously, not at lab time.
- Vendor Declaration path remains open for downstream product-family expansion.

### Negative / costs

- More handlers to ship in Phase 2 / 3. Smart Charging in particular is a substantial chunk of work (charging profiles, schedules, stacking — see Appendix C TC_056..TC_072).
- Reservations + LocalAuthList add Postgres tables and command paths beyond Core.
- Performance-measurement targets in Appendix A.2 (response time on Authorize, etc.) become production SLOs by virtue of the cert; missing them after cert is a regression.
- Without OCA membership we cannot run OCTT locally. **Obtaining OCA membership is now a project-critical dependency** — it gates task C-3 and therefore the entire cert track.

### Risks

- **OCTT failure at lab** — re-test required (per §5.5 of the procedure). Mitigation: run OCTT subset in CI from W2 (task C-3 onwards); never let OCTT fail silently.
- **Spec changes mid-flight** (errata sheet update) — unlikely on 1.6 (frozen), more likely on 2.0.1. Mitigation: pin `ocpp` library minor version (already done — `pyproject.toml`); subscribe to OCA errata announcements.
- **Performance degrades after cert** — possible if we don't track Authorize response time as an SLO. Mitigation: add the cert performance parameters to the Phase 4 SLO definitions (task E4-8).
- **OCA membership delayed** — fall-back is to run OCTT only at the lab during the cert sitting itself. Slower-feedback loop; higher chance of re-test. Document as an active risk in the conformance doc until membership lands.

### Reversibility

- **Profile selection is partially reversible.** Dropping Smart Charging or Reservations from the cert scope is a lab-paperwork change up until the cert is awarded; after that, narrowing the cert means recertifying against the smaller PICS.
- **DUT type (CSMS) is not reversible.** A Charging Station Software Stack would be a separate certification program, not a downscope.

## Project conventions implied by this decision

- Every handler MR cites the Appendix C test ID(s) it satisfies (e.g., `TC_001`, `TC_011_1`).
- The PICS for OCPP 1.6 (Appendix A.1 of the Certification Procedure) is maintained as a doc in this repo: see [`09-certification-readiness.md`](../09-certification-readiness.md).
- Performance-measurement parameters (Appendix A.2) are tracked from W6 (Phase 4 load test) onwards; values appear on the Phase 4 SLO dashboards.
- The conformance matrix in [`08-ocpp-conformance.md`](../08-ocpp-conformance.md) uses TC IDs as primary keys, not OCPP action names.

## References

- OCA OCPP 1.6 Certification Procedure (the document this ADR responds to). Place on the project shared drive — copyright held by OCA, do not commit to the repo.
- [OCPP 1.6 Edition 2 specification](https://www.openchargealliance.org/) (date: 2017-09-28) — task C-1.
- [OCPP 1.6 Errata sheet v4.0 Release](https://www.openchargealliance.org/) (date: 2019-10-23) — task C-1.
- [OCPP 1.6 security whitepaper Edition 3](https://www.openchargealliance.org/) (date: 2022-02-17) — task C-1.
- [ADR-0002 — Adopt mobilityhouse/ocpp](./0002-mobilityhouse-ocpp-library.md).
- [`docs/09-certification-readiness.md`](../09-certification-readiness.md) — the cert-readiness playbook.
