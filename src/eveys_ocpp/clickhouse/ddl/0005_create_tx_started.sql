-- tx.started — StartTransaction (E2-13, ADR-0020).
--
-- Mirrors `proto/events/v1/events.proto` `TxStarted`. The matching
-- "tx stopped" event is intentionally absent (see `events.proto`
-- header) — billing/CDR generation reads from Postgres
-- `transactions` row state, not from a Kafka stop event.
--
-- `meter_start_wh` matches the proto's `int64`; ClickHouse's `Int64`
-- handles values well past any plausible meter reading.
CREATE TABLE IF NOT EXISTS tx_started
(
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    transaction_id      Int64,
    connector_id        Int32,
    id_tag              String,
    meter_start_wh      Int64,
    charger_reported_at String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (cp_id, occurred_at);
