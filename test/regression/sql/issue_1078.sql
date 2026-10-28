-- POSIX regex operators are partial matches in Postgres. DuckDB's ~ is a
-- full match, so they have to be translated to regexp_matches().
CREATE TABLE t (s text);
INSERT INTO t VALUES ('App\\Models\\Product'), ('App\\Models\\Order'), ('product');

SET duckdb.force_execution = on;
-- single-process scans keep the test independent of worker availability
SET duckdb.max_workers_per_postgres_scan = 0;

SELECT s FROM t WHERE s ~ 'Product' ORDER BY s;
SELECT s FROM t WHERE s !~ 'Product' ORDER BY s;
SELECT s FROM t WHERE s ~* 'product' ORDER BY s;
SELECT s FROM t WHERE s !~* 'product' ORDER BY s;

-- anchored patterns keep working
SELECT s FROM t WHERE s ~ '^App.*Order$' ORDER BY s;

-- the operators also work in the select list and with non-literal patterns
SELECT s, s ~ 'Mod', s ~* 'PROD' FROM t ORDER BY s;
SELECT count(*) FROM t WHERE s ~ substring('xProductx' FROM 2 FOR 7);

-- varchar and char columns take the same path
CREATE TABLE t2 (v varchar(20), c char(10));
INSERT INTO t2 VALUES ('abc', 'abc');
SELECT v ~ 'b', c ~ 'b', v !~* 'B' FROM t2;

DROP TABLE t;
DROP TABLE t2;
