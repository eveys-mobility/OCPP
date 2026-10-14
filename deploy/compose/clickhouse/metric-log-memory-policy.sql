-- NOT an automatic migration. Review the existing table/global settings first.
-- No rows are deleted; the existing TTL and primary/index granularity remain.
-- New part format does not rewrite existing parts until they merge normally.
ALTER TABLE system.metric_log MODIFY SETTING
    min_bytes_for_wide_part=18446744073709551615,
    merge_max_block_size=1024;
