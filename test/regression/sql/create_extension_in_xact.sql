-- When CREATE EXTENSION pg_duckdb ran inside an explicit transaction block, the
-- metadata cache could be populated halfway through the extension script: at
-- that point the "duckdb" access method had not been created yet, so
-- table_am_oid was cached as InvalidOid (0). Because a sequence's relam is also
-- 0, IsDuckdbTable() then considered every sequence to be a DuckDB table, and a
-- plain ALTER SEQUENCE was wrongly routed into the DuckDB path, failing with
-- "Writing to DuckDB and Postgres tables in the same transaction block is not
-- supported".
DROP EXTENSION pg_duckdb CASCADE;

CREATE SEQUENCE public.create_extension_in_xact_seq;

BEGIN;
CREATE EXTENSION pg_duckdb;
-- Postgres-only operation on a sequence: must not be treated as a write to a
-- DuckDB table.
ALTER SEQUENCE public.create_extension_in_xact_seq OWNER TO CURRENT_USER;
-- DuckDB execution also works in the same transaction now that the cache is
-- correctly populated with the real "duckdb" access method OID. This is a read:
-- a DuckDB write here would be a genuinely unsupported mixed-write transaction,
-- because CREATE EXTENSION itself already wrote to Postgres catalogs.
SELECT count(*) FROM duckdb.query($$ SELECT 42 AS x $$);
COMMIT;

DROP SEQUENCE public.create_extension_in_xact_seq;
