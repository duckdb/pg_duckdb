CREATE TABLE t1(a INT, b INT, c TEXT);
INSERT INTO t1 SELECT g, g % 100, 'scan_potgres_table_'||g from generate_series(1,100000) g;

SET client_min_messages TO DEBUG1;

-- COUNT(*)
SELECT COUNT(*) FROM t1;

-- SEQ SCAN
SELECT COUNT(a) FROM t1 WHERE a < 10;

-- CREATE INDEX on t1
SET client_min_messages TO DEFAULT;
CREATE INDEX ON t1(a);
SET client_min_messages TO DEBUG1;

-- BITMAP INDEX
SELECT COUNT(a) FROM t1 WHERE a < 10;

-- INDEXONLYSCAN
SET enable_bitmapscan TO false;
SELECT COUNT(a) FROM t1 WHERE a = 1;

-- INDEXSCAN
SELECT COUNT(c) FROM t1 WHERE a = 1;

-- TEMPORARY TABLES JOIN WITH HEAP TABLES
SET client_min_messages TO DEFAULT;
CREATE TEMP TABLE t2(a int);
INSERT INTO t2 VALUES (1), (2), (3);
SET client_min_messages TO DEBUG1;

SELECT t1.a, t2.a FROM t1, t2 WHERE t1.a = t2.a;

-- JOIN WITH SAME TABLE (on WORKERS)
SELECT COUNT(*) FROM t1 AS t1_1, t1 AS t1_2 WHERE t1_1.a < 2 AND t1_2.a > 8;

-- JOIN WITH SAME TABLE (in BACKEND PROCESS)
SET max_parallel_workers TO 0;
SELECT COUNT(*) FROM t1 AS t1_1, t1 AS t1_2 WHERE t1_1.a < 2 AND t1_2.a > 8;
SET max_parallel_workers TO DEFAULT;


SET enable_bitmapscan TO DEFAULT;
SET client_min_messages TO DEFAULT;
DROP TABLE t1, t2;
