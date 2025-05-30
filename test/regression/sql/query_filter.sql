CREATE TABLE query_filter_int(a INT);
INSERT INTO query_filter_int SELECT g FROM generate_series(1,100) g;
SELECT COUNT(*) FROM query_filter_int WHERE a  <= 50;
DROP TABLE query_filter_int;

CREATE TABLE query_filter_float(a FLOAT8);
INSERT INTO query_filter_float VALUES (0.9), (1.0), (1.1);
SELECT COUNT(*) FROM query_filter_float WHERE a < 1.0;
SELECT COUNT(*) FROM query_filter_float WHERE a <= 1.0;
SELECT COUNT(*) FROM query_filter_float WHERE a < 1.1;
DROP TABLE query_filter_float;

CREATE TABLE query_filter_varchar(a VARCHAR);
INSERT INTO query_filter_varchar VALUES ('t1'), ('t2'), ('t1');
SELECT COUNT(*)FROM query_filter_varchar WHERE a = 't1';
SELECT COUNT(a) FROM query_filter_varchar WHERE a = 't1';
SELECT a, COUNT(*) FROM query_filter_varchar WHERE a = 't1' GROUP BY a;

INSERT INTO query_filter_varchar VALUES ('at1'), ('btt'), ('ttt');
SET duckdb.log_pg_explain = true;
-- Pushed down to PG executor
SELECT * FROM query_filter_varchar WHERE a LIKE '%t%';
SELECT * FROM query_filter_varchar WHERE a LIKE 't%';
SELECT * FROM query_filter_varchar WHERE a LIKE '%t';
RESET duckdb.log_pg_explain;

-- Not pushed down but making sure nothing's broken
SELECT * FROM query_filter_varchar WHERE a LIKE NULL;
SELECT * FROM query_filter_varchar WHERE NULL LIKE a;
SELECT * FROM query_filter_varchar WHERE a LIKE a;

DROP TABLE query_filter_varchar;

CREATE TABLE query_filter_output_column(a INT, b VARCHAR, c FLOAT8);
INSERT INTO query_filter_output_column VALUES (1, 't1', 1.0), (2, 't1', 2.0), (2, 't2', 1.0);
-- Projection ids list will be used (column `a`is not needed after scan)
SELECT b FROM query_filter_output_column WHERE a = 2;
-- Column ids list used because both of fetched column are used after scan
SELECT a, b FROM query_filter_output_column WHERE b = 't1';
-- Column ids list used because both of fetched column are used after scan.
-- Swapped order of table columns.
SELECT b, a FROM query_filter_output_column WHERE b = 't1';
-- Projection ids list will be used (column `b`is not needed after scan)
SELECT a, c FROM query_filter_output_column WHERE b = 't1';
-- All columns in tuple unordered
SELECT c, a, b FROM query_filter_output_column WHERE a = 2;
DROP TABLE query_filter_output_column;
