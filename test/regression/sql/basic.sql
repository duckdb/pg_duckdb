CREATE TABLE t(a INT);

INSERT INTO t SELECT g % 10 from generate_series(1,1000000) g;

SELECT COUNT(*) FROM t;
SELECT a, COUNT(*) FROM t WHERE a > 5 GROUP BY a ORDER BY a;

SET duckdb.max_threads_per_query to 4;

SELECT COUNT(*) FROM t;
SELECT a, COUNT(*) FROM t WHERE a > 5 GROUP BY a ORDER BY a;

CREATE TABLE empty(a INT);
SELECT COUNT(*) FROM empty;

SET duckdb.max_threads_per_query TO default;
SET client_min_messages TO default;

DROP TABLE t;
DROP TABLE empty;
