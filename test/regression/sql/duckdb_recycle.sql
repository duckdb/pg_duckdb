SET duckdb.execution = true;
CREATE TABLE ta(a INT);
EXPLAIN SELECT count(*) FROM ta;
SELECT duckdb.recycle_ddb();
EXPLAIN SELECT count(*) FROM ta;
DROP TABLE ta;
