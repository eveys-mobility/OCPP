-- cp.boot — BootNotification history (E2-13, ADR-0020).
--
-- Mirrors `proto/events/v1/events.proto` `CpBoot`. Replay-gated
-- inbound (E2-11): one row per logical boot, not per retry — the
-- gateway's idempotency cache absorbs the retry storm before the
-- envelope reaches Kafka.
--
-- `status` is the proto's `CpBootStatus` enum (CP_BOOT_STATUS_ACCEPTED
-- / _PENDING / _REJECTED) stored as the variant's name; same enum-as-
-- string convention used across all event-tables (ADR-0020).
CREATE TABLE IF NOT EXISTS cp_boot
(
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    vendor              String,
    model               String,
    firmware_version    String,
    serial_number       String,
    status              String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (cp_id, occurred_at);
