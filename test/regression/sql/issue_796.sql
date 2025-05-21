SET duckdb.force_execution = TRUE;

CREATE TEMP TABLE t2(a INT);
INSERT INTO t2 SELECT i from generate_series(1, 2048) as i; -- works with 2047
SELECT * FROM t2;
