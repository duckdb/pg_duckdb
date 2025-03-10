-- Test +/- inf values
CREATE TABLE t(a DATE);
INSERT INTO t VALUES('Infinity'), ('-Infinity');

-- PG Execution
SELECT * from t;
SELECT isfinite(a) FROM t;

set duckdb.force_execution = true;
-- DuckDB execution
SELECT * from t;
SELECT isfinite(a) FROM t;

-- Cleanup
set duckdb.force_execution = false;
DROP TABLE t;

-- Check upper and lower limits of date range
SELECT * FROM duckdb.query($$ SELECT  '4714-11-24 (BC)'::date as date $$);
SELECT * FROM duckdb.query($$ SELECT  '4714-11-23 (BC)'::date as date $$);  -- out of range
SELECT * FROM duckdb.query($$ SELECT  '5874897-12-31'::date as date $$);
SELECT * FROM duckdb.query($$ SELECT  '5874898-01-01'::date as date $$);  -- out of range
