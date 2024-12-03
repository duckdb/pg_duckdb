-- All the queries below should work even if duckdb.force_execution is turned off.
SET duckdb.force_execution = false;
CREATE TEMP TABLE t(
    bool BOOLEAN,
    i2 SMALLINT,
    i4 INT DEFAULT 1,
    i8 BIGINT NOT NULL,
    fl4 REAL DEFAULT random() + 1,
    fl8 DOUBLE PRECISION CHECK(fl8 > 0),
    t1 TEXT,
    t2 VARCHAR,
    t3 BPCHAR,
    d DATE,
    ts TIMESTAMP,
    json_obj JSON,
    CHECK (i4 > i2)
) USING duckdb;


INSERT INTO t VALUES (true, 2, 4, 8, 4.0, 8.0, 't1', 't2', 't3', '2024-05-04', '2020-01-01T01:02:03', '{"a": 1}');
SELECT * FROM t;

CREATE TEMP TABLE t_heap (a int);
INSERT INTO t_heap VALUES (2);

SELECT * FROM t JOIN t_heap ON i2 = a;

-- The default_table_access_method GUC should be honored.
set default_table_access_method = 'duckdb';
CREATE TEMP TABLE t2(a int);

INSERT INTO t2 VALUES (1), (2), (3);
SELECT * FROM t2 ORDER BY a;

DELETE FROM t2 WHERE a = 2;
SELECT * FROM t2 ORDER BY a;

UPDATE t2 SET a = 5 WHERE a = 3;
SELECT * FROM t2 ORDER BY a;

TRUNCATE t2;
SELECT * FROM t2 ORDER BY a;

-- Writing to a DuckDB table in a transaction is allowed
BEGIN;
    INSERT INTO t2 VALUES (1), (2), (3);
END;

-- We shouldn't be able to run DuckDB DDL in transactions (yet).
BEGIN;
    CREATE TEMP TABLE t3(a int);
END;

BEGIN;
    DROP TABLE t2;
END;

-- But plain postgres DDL and queries should work fine
BEGIN;
    CREATE TEMP TABLE t4(a int) USING heap;
    INSERT INTO t4 VALUES (1);
    SELECT * FROM t4;
    DROP TABLE t4;
END;

-- Even if duckdb.force_execution is turned on
BEGIN;
    SET LOCAL duckdb.force_execution = true;
    CREATE TEMP TABLE t4(a int) USING heap;
    INSERT INTO t4 VALUES (1);
    SELECT * FROM t4;
    DROP TABLE t4;
END;

-- ANALYZE should not fail on our tables. For now it doesn't do anything
-- though. But it should not fail, otherwise a plain "ANALYZE" of all tables
-- will error.
ANALYZE t;

SELECT duckdb.raw_query($$ SELECT database_name, schema_name, sql FROM duckdb_tables() $$);

DROP TABLE t, t_heap, t2;

SELECT duckdb.raw_query($$ SELECT database_name, schema_name, sql FROM duckdb_tables() $$);

CREATE TABLE t(a int);

-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE TEMP TABLE t(a int PRIMARY KEY);
-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE TEMP TABLE t(a int UNIQUE);
CREATE TEMP TABLE t(a int, b int GENERATED ALWAYS AS (a + 1) STORED);
CREATE TEMP TABLE t(a int GENERATED ALWAYS AS IDENTITY);
CREATE TEMP TABLE theap(b int PRIMARY KEY) USING heap;
CREATE TEMP TABLE t(a int REFERENCES theap(b));
DROP TABLE theap;
-- allowed but all other collations are not supported
CREATE TEMP TABLE t(a text COLLATE "default");
DROP TABLE t;
CREATE TEMP TABLE t(a text COLLATE "C");
DROP TABLE t;
CREATE TEMP TABLE t(a text COLLATE "POSIX");
DROP TABLE t;
CREATE TEMP TABLE t(a text COLLATE "de-x-icu");

CREATE TEMP TABLE t(A text COMPRESSION "pglz");

CREATE TEMP TABLE t(a int) WITH (fillfactor = 50);

CREATE TEMP TABLE cities_duckdb (
  name       text,
  population real,
  elevation  int
);

CREATE TEMP TABLE cities_heap (
  name       text,
  population real,
  elevation  int
) USING heap;

-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE TEMP TABLE capitals_duckdb (
  state      char(2) UNIQUE NOT NULL
) INHERITS (cities_duckdb);

-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE TEMP TABLE capitals_duckdb (
  state      char(2) UNIQUE NOT NULL
) INHERITS (cities_heap);

-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE TEMP TABLE capitals_heap (
  state      char(2) UNIQUE NOT NULL
) INHERITS (cities_duckdb);

DROP TABLE cities_heap, cities_duckdb;

CREATE TEMP TABLE t(a int) ON COMMIT PRESERVE ROWS;
INSERT INTO t VALUES (1);
SELECT * FROM t;
DROP TABLE t;
CREATE TEMP TABLE t(a int) ON COMMIT DELETE ROWS;
INSERT INTO t VALUES (1);
SELECT * FROM t;
DROP TABLE t;
-- unsupported
CREATE TEMP TABLE t(a int) ON COMMIT DROP;

-- CTAS fully in Duckdb
CREATE TEMP TABLE webpages USING duckdb AS SELECT * FROM read_csv('../../data/web_page.csv') as (column00 int, column01 text, column02 date);
SELECT * FROM webpages ORDER BY column00 LIMIT 2;

CREATE TEMP TABLE t_heap(a int) USING heap;
INSERT INTO t_heap VALUES (1);

-- CTAS from postgres table to duckdb table
CREATE TEMP TABLE t(b) USING duckdb AS SELECT * FROM t_heap;
SELECT * FROM t;

-- CTAS from DuckDB table to postgres table
CREATE TEMP TABLE t_heap2(c) USING heap AS SELECT * FROM t_heap;
SELECT * FROM t_heap2;

SELECT duckdb.raw_query($$ SELECT database_name, schema_name, sql FROM duckdb_tables() $$);

-- multi-VALUES
CREATE TEMP TABLE ta (a int DEFAULT 3, b int) USING duckdb;
INSERT INTO ta (b) VALUES (123), (456);
INSERT INTO ta (a, b) VALUES (123, 456), (456, 123);
SELECT * FROM ta;

CREATE TEMP TABLE tb (a int DEFAULT 3, b int, c varchar DEFAULT 'pg_duckdb') USING duckdb;
INSERT INTO tb (a) VALUES (123), (456);
INSERT INTO tb (b) VALUES (123), (456);
INSERT INTO tb (c) VALUES ('ta'), ('tb');
SELECT * FROM tb;

-- INSERT ... SELECT
TRUNCATE TABLE ta;
INSERT INTO ta (a) SELECT 789;
INSERT INTO ta (b) SELECT 789;
INSERT INTO ta (a) SELECT * FROM t_heap;
INSERT INTO ta (b) SELECT * FROM t_heap;
SELECT * FROM ta;
INSERT INTO ta (a) SELECT generate_series(1, 3); -- no support

TRUNCATE TABLE tb;
INSERT INTO tb (a) SELECT 789;
INSERT INTO tb (b) SELECT 789;
INSERT INTO tb (a) SELECT * FROM t_heap;
INSERT INTO tb (b) SELECT * FROM t_heap;
SELECT * FROM tb;

TRUNCATE TABLE tb;
INSERT INTO tb (c) SELECT 'ta';
INSERT INTO tb (c) SELECT 'ta' || 'tb';
INSERT INTO tb (a) SELECT (2)::numeric;
INSERT INTO tb (b) SELECT (3)::numeric;
INSERT INTO tb (c) SELECT t.a FROM (SELECT 'ta' || 'tb' AS a) t;
INSERT INTO tb (b, c) SELECT t.b, t.c FROM (SELECT (3)::numeric AS b, 'ta' || 'tb' AS c) t;
INSERT INTO tb (a, b, c) SELECT 1, 2, 'tb';
INSERT INTO tb  SELECT * FROM (SELECT (3)::numeric AS a, (3)::numeric AS b, 'ta' || 'tb' AS c) t;
SELECT * FROM tb;

DROP TABLE webpages, t, t_heap, t_heap2, ta, tb;
