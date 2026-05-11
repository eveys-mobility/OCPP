-- cp.offline_duration — per-CP offline window measured at reconnect.
--
-- Mirrors `proto/events/v1/events.proto` `CpOfflineDuration`. One row
-- per outage, written on the connect that closed it. The matching
-- `cp.connected` row in `cp_status` (if you correlate) shares the
-- envelope's `event_id` window but is a different topic.
--
-- Partition + order keys mirror the other event tables. We partition
-- on `came_online_at` (the row's anchor timestamp = envelope
-- `occurred_at`) so monthly retention drops align with the connect-
-- side time. `went_offline_at` is informational and not in the order
-- key — outage detection queries page by reconnect time.
CREATE TABLE IF NOT EXISTS cp_offline_duration
(
    event_id            String,
    occurred_at         DateTime64(3, 'UTC'),
    cp_id               String,
    schema_version      String,
    trace_id            String,

    went_offline_at     DateTime64(3, 'UTC'),
    came_online_at      DateTime64(3, 'UTC'),
    offline_seconds     Int64,
    prior_pod_id        String,
    prior_reason        String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(came_online_at)
ORDER BY (cp_id, came_online_at);
