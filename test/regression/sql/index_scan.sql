CREATE TABLE t(a INT PRIMARY KEY, b TEXT, c FLOAT);

INSERT INTO t SELECT g, 'abcde_'||g, g % 1000 FROM generate_series(1,1000000) g;

SET client_min_messages to 'DEBUG2';

SELECT b FROM t WHERE a = 10;

CREATE INDEX t_idx_mixed on t(a, c);

SET duckdb.execution TO FALSE;
EXPLAIN SELECT a,c, count(*) FROM t WHERE a < 1000 GROUP BY a,c ORDER BY a LIMIT 10;
SET duckdb.execution TO TRUE;

SELECT a,c, count(*) FROM t WHERE a = 10 GROUP BY a,c ORDER BY a LIMIT 10;

SET client_min_messages to default;

DROP TABLE t;
