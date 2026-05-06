-- Migration tracking table (E2-13 + E2-14, ADR-0020).
--
-- Mirrors Alembic's `alembic_version` shape so reviewers familiar with
-- Alembic recognize the pattern. Append-only — every row is a migration
-- that has already been applied successfully.
--
-- The loader (`migrate.py`) uses this to compute the list of pending
-- DDL files: any `NNNN_*.sql` whose `version` (parsed from the filename
-- prefix) does not appear here.
CREATE TABLE IF NOT EXISTS schema_migrations
(
    version    UInt32,
    name       String,
    applied_at DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY version;
