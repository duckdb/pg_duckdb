CREATE TABLE foo AS SELECT 'bar' AS t;
BEGIN; SET duckdb.force_execution = true; SELECT t::integer AS t1 FROM foo; ROLLBACK;
SET duckdb.force_execution = true;
SELECT 1 FROM foo;
