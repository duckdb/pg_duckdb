CREATE TABLE t(a TEXT);
INSERT INTO t VALUES ('a\');

set duckdb.force_execution = true;
SELECT a LIKE 'a\%' FROM t;
SELECT a ILIKE 'a\%' FROM t;
SELECT a NOT LIKE 'a\%' FROM t;
SELECT a NOT ILIKE 'a\%' FROM t;

DROP TABLE t;
