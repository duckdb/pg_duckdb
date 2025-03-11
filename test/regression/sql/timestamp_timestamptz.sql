----------------------------------------
-- Timestamp tests
----------------------------------------
-- Test +/- inf values
CREATE TABLE t(a TIMESTAMP);
INSERT INTO t VALUES('Infinity'), ('-Infinity');

-- PG Execution
SELECT * from t ORDER BY a;
SELECT isfinite(a) FROM t;

set duckdb.force_execution = true;
-- DuckDB execution
SELECT * from t ORDER BY a;
SELECT isfinite(a) FROM t;

-- Cleanup
set duckdb.force_execution = false;
DROP TABLE t;

SELECT * FROM duckdb.query($$ SELECT '4714-11-24 (BC) 00:00:00'::timestamp_s as timestamp_s $$);
SELECT * FROM duckdb.query($$ SELECT '4714-11-23 (BC) 23:59:59'::timestamp_s as timestamp_s $$);  -- out of range
SELECT * FROM duckdb.query($$ SELECT '294246-12-31 23:59:59'::timestamp_s as timestamp_s $$);
SELECT * FROM duckdb.query($$ SELECT '294247-01-01 00:00:00'::timestamp_s as timestamp_s $$);  -- out of range

SELECT * FROM duckdb.query($$ SELECT '4714-11-24 (BC) 00:00:00.000000'::timestamp as timestamp $$);
SELECT * FROM duckdb.query($$ SELECT '4714-11-23 (BC) 23:59:59.999999'::timestamp as timestamp $$);  -- out of range
SELECT * FROM duckdb.query($$ SELECT '294246-12-31 23:59:59.999999'::timestamp as timestamp $$);
SELECT * FROM duckdb.query($$ SELECT '294247-01-01 00:00:00.000000'::timestamp as timestamp $$);  -- out of range

SELECT * FROM duckdb.query($$ SELECT '4714-11-24 (BC) 00:00:00.000'::timestamp_ms as timestamp_ms $$);
SELECT * FROM duckdb.query($$ SELECT '4714-11-23 (BC) 23:59:59.999'::timestamp_ms as timestamp_ms $$);  -- out of range
SELECT * FROM duckdb.query($$ SELECT '294246-12-31 23:59:59.999'::timestamp_ms as timestamp_ms $$);
SELECT * FROM duckdb.query($$ SELECT '294247-01-01 00:00:00.000'::timestamp_ms as timestamp_ms $$);  -- out of range
----------------------------------------
-- TimestampTz tests
----------------------------------------
-- Test +/- inf valuestz
CREATE TABLE t(a TIMESTAMPTZ);
INSERT INTO t VALUES('Infinity'), ('-Infinity');

-- PG Execution
SELECT * from t ORDER BY a;
SELECT isfinite(a) FROM t;

set duckdb.force_execution = true;
-- DuckDB execution
SELECT * from t ORDER BY a;
SELECT isfinite(a) FROM t;

-- Cleanup
set duckdb.force_execution = false;
DROP TABLE t;

SELECT * FROM duckdb.query($$ SELECT '4714-11-24 (BC) 00:00:00'::timestamptz as timestamptz $$);
SELECT * FROM duckdb.query($$ SELECT '4714-11-23 (BC) 23:59:59'::timestamptz as timestamptz $$);  -- out of range
SELECT * FROM duckdb.query($$ SELECT '294246-12-31 23:59:59'::timestamptz as timestamptz $$);
SELECT * FROM duckdb.query($$ SELECT '294247-01-01 00:00:00'::timestamptz as timestamptz $$);  -- out of range
