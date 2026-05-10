# Changelog

All notable changes to the gateway land here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versioning: [SemVer](https://semver.org/) once a tag is cut; until then everything sits under `[Unreleased]`.

This file starts at the merge that introduced it; for anything earlier, the git log is authoritative.

## [Unreleased]

### Added

- **Per-transaction telemetry** on `GET /api/v1/transactions/{transaction_id}`. Response now carries a bounded `telemetry` block with SoC start/last percent and a per-phase voltage/current/power snapshot for `L1`/`L2`/`L3`. ClickHouse-backed; absent phases / `null` SoC fields when the charger never reported that dimension. List endpoints intentionally omit `telemetry` (would be N+1 fan-out per cursor row); use the detail endpoint per id, or `/meter-values?transaction_id=…` for the full curve. Contract: `docs/integration/02-gateway-rest-api.md`. (#134)

### Fixed

- **`MeterValues` enum capture.** The handler dropped every OCPP 1.6 enum dimension on the floor — `measurand`, `phase`, `unit`, `context`, `format`, `location` all landed in storage as `*_UNSPECIFIED` regardless of what the charger sent. Existing `/meter-values` consumers filtering on `?measurand=Voltage` (or any other measurand) silently matched nothing. Now translates the wire form to proto enum values; vendor-extension strings still fall through to `*_UNSPECIFIED` and the raw `value` is preserved. Also applies the OCPP 1.6 §6.21.4 default (absent `measurand` → `Energy.Active.Import.Register`). (#135)
- **Storage ↔ API enum naming.** ClickHouse stores the proto enum name (`MEASURAND_VOLTAGE`, `PHASE_L1`); the REST API now consistently exposes the OCPP 1.6 wire form (`Voltage`, `L1`). Translation happens at the route boundary in both directions: `?measurand=` query params translate wire → proto name on input, response fields translate back on output. `*_UNSPECIFIED` and unmapped vendor-extension strings surface as `null` rather than leak the internal sentinel. Closes the loop on the telemetry feature — `telemetry.phases.{L1,L2,L3}` actually populates against a real charger now. (#136)
