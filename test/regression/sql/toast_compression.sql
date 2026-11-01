-- pg_duckdb decompresses TOAST values itself. Test both compression methods,
-- with a value that is small once compressed (kept inline) and one that is
-- still large (moved to the TOAST table).
CREATE TABLE toast_pglz (v text COMPRESSION pglz);
CREATE TABLE toast_lz4 (v text COMPRESSION lz4);

INSERT INTO toast_pglz VALUES
    (repeat('abc', 100000)),
    ((SELECT string_agg(md5((i % 100)::text), '') FROM generate_series(1, 2000) i));
INSERT INTO toast_lz4 SELECT v FROM toast_pglz;

-- How Postgres stored them
SET duckdb.force_execution = false;
SELECT 'pglz' AS method, pg_column_compression(v), length(v), md5(v) FROM toast_pglz
UNION ALL
SELECT 'lz4', pg_column_compression(v), length(v), md5(v) FROM toast_lz4
ORDER BY 1, 3;

-- DuckDB reads the same values
SET duckdb.force_execution = true;
SELECT length(v), md5(v) FROM toast_pglz ORDER BY 1;
SELECT length(v), md5(v) FROM toast_lz4 ORDER BY 1;

DROP TABLE toast_pglz, toast_lz4;
