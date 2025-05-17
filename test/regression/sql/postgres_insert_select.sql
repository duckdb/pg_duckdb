
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
INSERT INTO tbl select r['a']::int, r['b'] from duckdb.query($$ SELECT 1 a, 'abc' b $$) r;
INSERT INTO tbl select r['a']::int, r['b'] from duckdb.query($$ SELECT 11 a, 'def' b $$) r;
INSERT INTO tbl select r['a']::int, r['b'] from duckdb.query($$ SELECT 21 a, 'ghi' b $$) r;
SELECT * FROM tbl_p1 ORDER BY a;
SELECT * FROM tbl_p2 ORDER BY a;
DROP TABLE tbl;

-- case: INSERT INTO TABLE (col1, col3)
CREATE TABLE tbl (a int PRIMARY KEY, b text, c int DEFAULT 10);
INSERT INTO tbl (a, c) SELECT i, 20 FROM generate_series(1, 3) i;
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(4, 6) i;
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

-- case: UPDATE/DELETE should be blocked
SET duckdb.log_pg_explain to on;
CREATE TABLE tbl (a int PRIMARY KEY, b text);
INSERT INTO tbl (a, b) SELECT i, 'foo' FROM generate_series(1, 3) i;
UPDATE tbl SET b = 'bar' WHERE a = 1;
DELETE FROM tbl WHERE a = 1;
-- INSERT without subquery should also be blocked
INSERT INTO tbl (a, b) VALUES (1, 'foo');
DROP TABLE tbl;
SET duckdb.log_pg_explain to off;

-- case: UNSUPPORTED TYPE
CREATE TABLE tbl (a int, b xml);
CREATE TABLE tbl1 (a int, b xml);
INSERT INTO tbl (a, b) VALUES (1, '<xml>foo</xml>');
SET duckdb.log_pg_explain to on;
INSERT INTO tbl1 SELECT * FROM tbl;
DROP TABLE tbl, tbl1;
SET duckdb.log_pg_explain to off;

-- case: query with JOIN
CREATE TABLE tbl (a int, b text);
CREATE TABLE tbl1 (a int, b1 text, b2 text);
INSERT INTO tbl (a, b) VALUES (1, 'foo'), (2, 'bar'), (1, 'baz');
EXPLAIN INSERT INTO tbl1 SELECT a.a, a.b, b.b FROM tbl a JOIN tbl b ON a.a = b.a;
INSERT INTO tbl1 SELECT a.a, a.b, b.b FROM tbl a JOIN tbl b ON a.a = b.a;
SET duckdb.log_pg_explain to on;
-- The following query should be blocked because of VALUE RTE in the subquery
INSERT INTO tbl1 SELECT a.a, a.b, b.column2 FROM tbl a JOIN (VALUES (2, 'yoyo'), (4, 'yoyo2')) AS b(column1, column2) ON a.a = b.column1;
SELECT * FROM tbl1 ORDER BY 1, 2, 3;
DROP TABLE tbl, tbl1;
