set duckdb.force_execution TO FALSE;
CREATE TABLE s (a text[]);
INSERT INTO s VALUES (ARRAY['abc', 'def', 'ghi']);
-- Because the next table is created using a CTAS, attndims is set to 0. That
-- confused us in the past. See #556 for details. We assume it's single
-- dimensional now.
CREATE TABLE t AS TABLE s;
SELECT * FROM s;
SELECT * FROM t;
SET duckdb.force_execution TO true;

-- PostgreSQL array constants use '{...}' syntax. They need to be deparsed as
-- DuckDB list literals when the query is executed by DuckDB (#1040).
SELECT 'integer[]' AS type, value::text
FROM (SELECT '{1,2,3}'::integer[] AS value) s;
SELECT 'integer[] with NULL' AS type, value::text
FROM (SELECT '{1,NULL,3}'::integer[] AS value) s;
SELECT 'bigint[]' AS type, value::text
FROM (SELECT '{1,2,3}'::bigint[] AS value) s;
SELECT 'numeric[]' AS type, value::text
FROM (SELECT '{10.50,20.25}'::numeric[] AS value) s;
SELECT 'date[]' AS type, value::text
FROM (SELECT '{2025-09-01,2026-09-01}'::date[] AS value) s;
SELECT 'text[]' AS type, value::text
FROM (SELECT ARRAY['comma,value', 'quote''value']::text[] AS value) s;
SELECT 'boolean[]' AS type, value::text
FROM (SELECT '{t,f}'::boolean[] AS value) s;
SELECT 'empty integer[]' AS type, value::text
FROM (SELECT '{}'::integer[] AS value) s;
SELECT 'three-dimensional integer[]' AS type, value::text
FROM (SELECT '{{{1,2},{3,4}},{{5,6},{7,8}}}'::integer[] AS value) s;
SELECT 1 = ANY ('{1,2,3}'::integer[]) AS any_integer;

SELECT * FROM s;
SELECT * FROM t;

-- Processing arryas of different dimensions in the same column is not something
-- that DuckDB can handle.
INSERT INTO s VALUES(ARRAY[['a', 'b'],['c','d']]);
SELECT * FROM s;
TRUNCATE s;

-- And we assume that the table metadata is correct about the dimensionality.
-- So even if the stored dimensionality is consistently wrong we will throw an
-- error.
INSERT INTO s VALUES(ARRAY[['a', 'b'],['c','d']]);
SELECT * FROM s;
-- But if you change the definition of the table, we will be able to handle it.
ALTER TABLE s ALTER COLUMN a SET DATA TYPE text[][];
SELECT * FROM s;

-- Similarly Posgres cannot support nested lists where different sub-lists at
-- the same level have a different length.
SELECT * FROM duckdb.query($$ SELECT ARRAY[ARRAY[1,2], ARRAY[3,4,5]] arr $$);

DROP TABLE s, t;
