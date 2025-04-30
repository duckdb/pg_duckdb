
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
