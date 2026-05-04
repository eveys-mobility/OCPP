-- cp.status — StatusNotification per-state-transition history (E2-13, ADR-0020).
--
-- Mirrors `proto/events/v1/events.proto` `CpStatus`. Latest status is
-- also kept in Postgres (`charge_points.last_status`) as a debug aid;
-- the historical sequence lives here.
--
-- Status / error_code / vendor_* are all `String` rather than enum
-- columns: OCPP 1.6 enumerates `status` (Available, Preparing,
-- Charging, SuspendedEV, SuspendedEVSE, Finishing, Reserved,
-- Unavailable, Faulted) but vendors emit free-form `vendor_id` /
-- `vendor_error_code` values that don't fit any closed enum.
CREATE TABLE IF NOT EXISTS cp_status
(
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    connector_id        Int32,
    status              String,
    error_code          String,
    info                String,
    vendor_id           String,
    vendor_error_code   String,
    charger_reported_at String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (cp_id, occurred_at);
