-- cp.ocpp_frames — bidirectional OCPP frame audit.
--
-- Mirrors `proto/events/v1/events.proto` `CpOcppFrame`. One row per
-- OCPP frame, both directions: `direction='inbound'` is CP→gateway,
-- `direction='outbound'` is gateway→CP. The frame's raw JSON is
-- stored verbatim in `raw_payload` so disputes and fraud
-- investigations can replay the exact bytes that were on the wire.
--
-- `transaction_id` is extracted by the ingestor when present in the
-- frame body (StartTransaction.conf, MeterValues, StopTransaction)
-- and stored alongside; null for frames that don't carry a tx
-- (BootNotification, Heartbeat, StatusNotification, etc). A
-- skip-index on the column makes "all frames for tx 12345" cheap
-- even though most rows have null in that column.
--
-- Order key matches sibling tables. The skip-index on
-- `transaction_id` keeps tx lookups partition-scoped — operators
-- almost always know which cp_id the tx is on (it's in the
-- transactions table; the API joins them), so cp_id stays the
-- primary access path. The skip-index is a belt-and-braces speedup
-- for the cross-cp "find by tx alone" case.

CREATE TABLE IF NOT EXISTS cp_ocpp_frames
(
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    direction           String,
    raw_payload         String,
    message_id          String,
    action              String,
    -- 2 = CALL, 3 = CALLRESULT, 4 = CALLERROR, 0 = unparseable.
    message_type        Int32,
    ocpp_version        String,
    -- Extracted by the ingestor from the OCPP payload when present.
    -- Nullable because most frame kinds don't carry a transaction.
    transaction_id      Nullable(Int64),

    INDEX idx_tx (transaction_id) TYPE minmax GRANULARITY 4,
    INDEX idx_action (action) TYPE bloom_filter GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (cp_id, occurred_at);
