
-- case: SELECT * FROM READ_CSV
CREATE TABLE tbl (sepal_length float, variety varchar);
INSERT INTO tbl SELECT r['sepal.length']::float, r['variety']::varchar FROM read_csv('../../data/iris.csv') r;
SELECT sepal_length, variety FROM tbl ORDER BY sepal_length LIMIT 5;
DROP TABLE tbl;

-- case: SELECT * FROM READ_JSON
CREATE TABLE tbl (a int PRIMARY KEY, b varchar, c real);
INSERT INTO tbl SELECT r['a']::int, r['b']::varchar, r['c']::real FROM read_json('../../data/table.json') r;
SELECT * FROM tbl ORDER BY a LIMIT 5;
-- DO IT AGAIN TO VALIDATE PK CONSTRAINT
INSERT INTO tbl SELECT r['a']::int, r['b']::varchar, r['c']::real FROM read_json('../../data/table.json') r;
DROP TABLE tbl;

-- case: INSERT INTO PARTITION TABLE
CREATE TABLE tbl (a int PRIMARY KEY, b text) PARTITION BY RANGE (a);
CREATE TABLE tbl_p1 PARTITION OF tbl FOR VALUES FROM (1) TO (10);
CREATE TABLE tbl_p2 PARTITION OF tbl FOR VALUES FROM (10) TO (20);
INSERT INTO tbl select r['a']::int, r['b']::text from duckdb.query($$ SELECT 1 a, 'abc' b $$) r;
INSERT INTO tbl select r['a']::int, r['b']::text from duckdb.query($$ SELECT 11 a, 'def' b $$) r;
INSERT INTO tbl select r['a']::int, r['b']::text from duckdb.query($$ SELECT 21 a, 'ghi' b $$) r;
SELECT * FROM tbl_p1 ORDER BY a;
SELECT * FROM tbl_p2 ORDER BY a;
DROP TABLE tbl;

-- case: INSERT INTO TABLE (col1, col3)
CREATE TABLE tbl (a int PRIMARY KEY, b text, c int DEFAULT 10);
INSERT INTO tbl (a, c) SELECT i, 20 FROM generate_series(1, 3) i;
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(4, 6) i;
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: INSERT INTO TABLE (col2, col1), i.e. columns in a different order
-- than the table definition
CREATE TABLE tbl (a int, b text, c int);
INSERT INTO tbl (b, a) SELECT r['x']::text, r['y']::int FROM duckdb.query($$ SELECT 'hello' x, 42 y $$) r;
INSERT INTO tbl (c, a, b) SELECT r['x']::int, r['y']::int, r['z']::text FROM duckdb.query($$ SELECT 1 x, 2 y, 'abc' z $$) r;
SELECT * FROM tbl ORDER BY a;
DROP TABLE tbl;

-- case: table with a dropped column
CREATE TABLE tbl (a int, b text, c int);
ALTER TABLE tbl DROP COLUMN b;
INSERT INTO tbl (c, a) SELECT r['x']::int, r['y']::int FROM duckdb.query($$ SELECT 1 x, 2 y $$) r;
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: values that don't fit the column type throw an error, just like
-- they would with Postgres execution
CREATE TABLE tbl (v varchar(3));
INSERT INTO tbl SELECT r['s']::text FROM duckdb.query($$ SELECT 'abcdef' s $$) r;
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: GENERATED and default columns are computed for rows produced by DuckDB
CREATE TABLE tbl (a int, b int GENERATED ALWAYS AS (a * 2) STORED, c serial);
INSERT INTO tbl (a) SELECT r['a']::int FROM duckdb.query($$ SELECT 21 a UNION ALL SELECT 33 $$) r;
SELECT * FROM tbl ORDER BY a;
DROP TABLE tbl;

-- case: prepared statements with parameters
CREATE TABLE tbl (a int, b text);
PREPARE heap_insert(int) AS INSERT INTO tbl SELECT r['a']::int, r['b']::text FROM duckdb.query($$ SELECT 1 a, 'foo' b UNION ALL SELECT 2, 'bar' $$) r WHERE r['a']::int = $1;
EXECUTE heap_insert(1);
EXECUTE heap_insert(2);
EXECUTE heap_insert(2);
SELECT * FROM tbl ORDER BY a;
DEALLOCATE heap_insert;
DROP TABLE tbl;

-- case: CTEs attached to the INSERT are moved into the SELECT that DuckDB
-- executes
CREATE TABLE tbl (a int, b text);
WITH x AS (SELECT r['a']::int a, r['b']::text b FROM duckdb.query($$ SELECT 1 a, 'basic' b $$) r)
INSERT INTO tbl SELECT * FROM x;
-- Also when they reference each other, are recursive, or when the SELECT has
-- CTEs of its own
WITH x AS (SELECT r['a']::int a FROM duckdb.query($$ SELECT 2 a $$) r),
     y AS (SELECT a, 'chained' FROM x)
INSERT INTO tbl SELECT * FROM y;
WITH RECURSIVE fib(a, b) AS (
    SELECT 3, 'rec' UNION ALL SELECT a + 1, b FROM fib WHERE a < 5
)
INSERT INTO tbl SELECT a, b FROM fib, duckdb.query($$ SELECT 1 $$) r;
WITH x AS (SELECT r['a']::int a FROM duckdb.query($$ SELECT 6 a $$) r)
INSERT INTO tbl WITH y AS (SELECT a, 'both' FROM x) SELECT * FROM y;
-- CTEs inside the SELECT itself also work on their own
INSERT INTO tbl SELECT * FROM (WITH x AS (SELECT r['a']::int a, r['b']::text b FROM duckdb.query($$ SELECT 7 a, 'sub' b $$) r) SELECT * FROM x) sub;
-- A CTE that shadows a real table reads from the CTE, not the table
CREATE TABLE x (a int, b text);
INSERT INTO x VALUES (99, 'WRONG');
WITH x AS (SELECT r['a']::int a, 'shadow'::text b FROM duckdb.query($$ SELECT 8 a $$) r)
INSERT INTO tbl SELECT * FROM x;
-- If the same CTE name is used on both the INSERT and the SELECT, DuckDB
-- errors on the duplicate name instead of silently picking one
WITH x AS (SELECT r['a']::int a, 'outer'::text b FROM duckdb.query($$ SELECT 9 a $$) r)
INSERT INTO tbl WITH x AS (SELECT 9, 'inner') SELECT * FROM x;
SELECT * FROM tbl ORDER BY a;
DROP TABLE tbl, x;

-- case: EXPLAIN ANALYZE is not supported, because it would silently insert
-- nothing while a Postgres-executed EXPLAIN ANALYZE actually inserts the rows
CREATE TABLE tbl (a int);
EXPLAIN (ANALYZE, COSTS OFF) INSERT INTO tbl SELECT r['a']::int FROM duckdb.query($$ SELECT 1 a $$) r;
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: RETURNING
CREATE TABLE tbl (a int PRIMARY KEY, b text);
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(1, 3) i RETURNING a, b;
DROP TABLE tbl;

-- case: ON CONFLICT DO UPDATE
CREATE TABLE tbl (a int PRIMARY KEY, b text);
INSERT INTO tbl SELECT i, 'foo' FROM generate_series(1, 3) i;
INSERT INTO tbl (a, b) SELECT i, 'qux' FROM generate_series(1, 3) i ON CONFLICT (a) DO UPDATE SET b = 'qux';
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: ON CONFLICT DO NOTHING
CREATE TABLE tbl (a int PRIMARY KEY, b text);
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(1, 2) i;
INSERT INTO tbl (a, b) SELECT i, 'qux' FROM generate_series(1, 4) i ON CONFLICT DO NOTHING;
SELECT * FROM tbl;
DROP TABLE tbl;

CREATE TABLE tbl (a INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, b text);
INSERT INTO tbl (b) SELECT 'foo' FROM generate_series(1, 2);
INSERT INTO tbl (b) SELECT 'qux' FROM generate_series(1, 4);;
SELECT * FROM tbl;
DROP TABLE tbl;

CREATE TABLE tbl (a SERIAL PRIMARY KEY, b text);
INSERT INTO tbl (b) SELECT 'foo' FROM generate_series(1, 2);
INSERT INTO tbl (b) SELECT 'qux' FROM generate_series(1, 4);;
SELECT * FROM tbl;
DROP TABLE tbl;

-- case: ARRAY / JSON type
CREATE TABLE tbl (a int, b text[], c jsonb);
CREATE TABLE tbl1 (a int, b text[], c jsonb);
INSERT INTO tbl (a, b, c) VALUES (1, ARRAY ['foo', 'bar'], '{"a": 1, "b": 2}');
INSERT INTO tbl1 SELECT * FROM tbl;
SELECT * FROM tbl1;
DROP TABLE tbl, tbl1;

-- case: UPDATE/DELETE and INSERT ... VALUES fall back to Postgres execution
CREATE TABLE tbl (a int PRIMARY KEY, b text);
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(1, 3) i;
UPDATE tbl SET b = 'bar' WHERE a = 1;
DELETE FROM tbl WHERE a = 2;
INSERT INTO tbl (a, b) VALUES (4, 'baz');
SELECT * FROM tbl ORDER BY a;
-- But when such statements require DuckDB execution, falling back to Postgres
-- is not possible, so they throw an error instead.
UPDATE tbl SET b = r['x']::text FROM duckdb.query($$ SELECT 'bar' x $$) r WHERE a = 1;
DELETE FROM tbl USING duckdb.query($$ SELECT 1 x $$) r WHERE a = r['x']::int;
INSERT INTO tbl VALUES (5, (SELECT r['x']::text FROM duckdb.query($$ SELECT 'quux' x $$) r));
DROP TABLE tbl;

-- case: UNSUPPORTED TYPE
CREATE TABLE tbl (a int, b xml);
CREATE TABLE tbl1 (a int, b xml);
INSERT INTO tbl (a, b) VALUES (1, '<xml>foo</xml>');
-- This falls back to Postgres execution, because DuckDB doesn't support the
-- xml type.
INSERT INTO tbl1 SELECT * FROM tbl;
SELECT * FROM tbl1;
-- When DuckDB execution is required we throw an error instead.
INSERT INTO tbl1 SELECT r['a']::int, NULL FROM duckdb.query($$ SELECT 1 a $$) r;
DROP TABLE tbl, tbl1;

-- case: query with JOIN
CREATE TABLE tbl (a int, b text);
CREATE TABLE tbl1 (a int, b1 text, b2 text);
INSERT INTO tbl (a, b) VALUES (1, 'foo'), (2, 'bar'), (1, 'baz');
-- We don't EXPLAIN this INSERT, because the DuckDB plan that would be
-- included in the output is different for Debug and Release builds of DuckDB.
INSERT INTO tbl1 SELECT a.a, a.b, b.b FROM tbl a JOIN tbl b ON a.a = b.a;
SET duckdb.log_pg_explain to on;
-- A query with a VALUES RTE in the subquery falls back to Postgres execution
INSERT INTO tbl1 SELECT a.a, a.b, b.column2 FROM tbl a JOIN (VALUES (2, 'yoyo'), (4, 'yoyo2')) AS b(column1, column2) ON a.a = b.column1;
SELECT * FROM tbl1 ORDER BY 1, 2, 3;
SET duckdb.log_pg_explain to off;
-- But when DuckDB execution is required, the VALUES RTE is accepted and the
-- whole SELECT runs in DuckDB.
INSERT INTO tbl1 SELECT r['a']::int, r['b']::text, b.column2 FROM duckdb.query($$ SELECT 2 a, 'x' b $$) r JOIN (VALUES (2, 'yoyo')) AS b(column1, column2) ON r['a']::int = b.column1;
SELECT * FROM tbl1 WHERE b1 = 'x';
DROP TABLE tbl, tbl1;
