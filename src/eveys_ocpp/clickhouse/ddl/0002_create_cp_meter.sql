-- cp.meter — high-volume MeterValues firehose (E2-13, ADR-0020).
--
-- Mirrors `proto/events/v1/events.proto` `CpMeter` inside the
-- `EventEnvelope` (ADR-0015). `SampledValue` is a repeated field on
-- the proto and lands here as ClickHouse's native `Nested` type:
-- one row per envelope, parallel arrays under `sampled_values.*`.
--
-- Query pattern:
--     SELECT cp_id, sv.value, sv.measurand, sv.unit
--     FROM cp_meter
--     ARRAY JOIN sampled_values AS sv
--     WHERE cp_id = 'CP_FOO'
--       AND occurred_at >= now() - INTERVAL 1 HOUR;
--
-- Partition + order key (ADR-0020 § "Project conventions implied"):
-- monthly partitions on the trustworthy server-receive timestamp;
-- ordered (cp_id, occurred_at) so per-charger time-range scans stay
-- cheap.
--
-- Proto enums (`Context`, `Format`, `Measurand`, `Phase`, `Location`,
-- `Unit`) are stored as their string names — not `Enum16` columns —
-- so future enum additions don't break inserts and ad-hoc queries
-- read like the proto.
--
-- `charger_reported_at` is kept as a `String` (not parsed to
-- `DateTime64`): charger clocks are untrusted (AGENTS rule 7); the
-- envelope's `occurred_at` is the authoritative time.
CREATE TABLE IF NOT EXISTS cp_meter
(
    -- envelope (ADR-0015) — same columns on every event-table
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    -- payload — top-level
    connector_id        Int32,
    transaction_id      Int64,
    charger_reported_at String,

    -- payload — repeated SampledValue, stored as a Nested column
    sampled_values Nested
    (
        value     String,
        context   String,
        format    String,
        measurand String,
        phase     String,
        location  String,
        unit      String
    )
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (cp_id, occurred_at);
