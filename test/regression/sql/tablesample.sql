SET duckdb.force_execution = true;

CREATE TABLE t (a INT);
INSERT INTO t SELECT i FROM generate_series(1, 10) AS i;

SELECT * FROM t TABLESAMPLE SYSTEM (100);
DROP TABLE t;
