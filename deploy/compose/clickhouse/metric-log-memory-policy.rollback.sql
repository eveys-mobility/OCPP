-- NOT an automatic rollback. Valid only after confirming that these settings
-- were not explicitly overridden on the old table (save SHOW CREATE TABLE).
-- Restore the old config file too; keep ingestion paused if Code241 returns.
ALTER TABLE system.metric_log RESET SETTING
    min_bytes_for_wide_part,
    merge_max_block_size;
