SELECT duckdb.raw_query($$ CREATE TABLE t(a int) $$);
SELECT duckdb.raw_query($$ SELECT * FROM duckdb_tables() $$);
