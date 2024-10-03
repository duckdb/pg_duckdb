CREATE TABLE foo AS SELECT 'bar' AS t;
BEGIN; SET duckdb.execution = true; SELECT t::integer AS t1 FROM foo; ROLLBACK;
SET duckdb.execution = true;
SELECT 1 FROM foo;
