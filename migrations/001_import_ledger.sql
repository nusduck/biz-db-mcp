-- 001_import_ledger.sql
--
-- Persistent idempotency ledger for the whitelisted artifact import
-- (`import_rows`). Apply this migration before enabling imports.
--
-- The service never runs DDL: startup only verifies that this InnoDB table
-- exists and has a unique index exactly on (idempotency_key). If the table is
-- missing or malformed, imports stay disabled with the message
-- "import ledger is missing or malformed; apply migrations/001_import_ledger.sql".
--
-- One ledger row and all of an import's business rows commit in the same
-- MySQL transaction. The unique key on (idempotency_key) makes retrying the
-- same institution + database + table + content digest + mode a no-op that
-- returns the already committed batch. `uk_batch` gives every batch a stable
-- id for audit and support.

CREATE TABLE IF NOT EXISTS bizdb_import_ledger (
    id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    idempotency_key CHAR(64) NOT NULL,
    batch_id CHAR(32) NOT NULL,
    institution_code VARCHAR(64) NOT NULL,
    database_id VARCHAR(64) NOT NULL,
    table_name VARCHAR(129) NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    mode VARCHAR(16) NOT NULL,
    row_count INT UNSIGNED NOT NULL,
    inserted_rows INT UNSIGNED NULL,
    updated_rows INT UNSIGNED NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    UNIQUE KEY uk_idempotency (idempotency_key),
    UNIQUE KEY uk_batch (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ---------------------------------------------------------------------------
-- Least-privilege GRANT template for the import account (DBA applies it).
--
-- The import connection must use a dedicated MySQL account that is never used
-- for the generic query/execute tools. Grant only:
--   * SELECT, INSERT, UPDATE on each whitelisted import table (no DELETE);
--   * SELECT, INSERT, UPDATE on the ledger (the service never deletes rows);
--   * nothing else: no DDL (CREATE/ALTER/DROP), no TRUNCATE, no DELETE, no
--     privileges on other business tables or schemas.
--
-- Repeat the first GRANT for every table listed in `imports.tables`. Replace
-- the schema, table names, account name, and host with the deployed values.
-- Password provisioning and credential rotation stay outside this migration
-- (DBPM supplies the password at startup); do not commit real credentials.
--
--   GRANT SELECT, INSERT, UPDATE ON `app`.`report_results`
--       TO 'bizdb_importer'@'db-app.internal';
--   GRANT SELECT, INSERT, UPDATE ON `app`.`bizdb_import_ledger`
--       TO 'bizdb_importer'@'db-app.internal';
--   -- Optional verification (run as the import account):
--   --   SHOW GRANTS FOR CURRENT_USER();
--
-- Notes:
--   * The ledger must live in the database named by the import credential's
--     DSN, and `databases.<id>.ledger_table` must match this table name.
--   * `table_name` stores the canonical lower-case `schema.table` (129 chars
--     covers a 64-character schema plus dot plus 64-character table).
--   * Do not add a BEFORE/AFTER trigger to import tables: the service
--     refuses any business table with a trigger. Keep the ledger trigger-free
--     as well; its writes assume no side effects.
--   * Do not replace the unique indexes with non-unique indexes; startup
--     verification will reject the ledger and disable imports.
