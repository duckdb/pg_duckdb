-- All the queries below should work even if duckdb.execution is turned off.
SET duckdb.execution = false;
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

-- We shouldn't be able to run DuckDB queries in transactions (yet).
BEGIN;
    INSERT INTO t2 VALUES (1), (2), (3);
END;

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

-- Even if duckdb.execution is turned on
BEGIN;
    SET LOCAL duckdb.execution = true;
    CREATE TEMP TABLE t4(a int) USING heap;
    INSERT INTO t4 VALUES (1);
    SELECT * FROM t4;
    DROP TABLE t4;
END;

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

DROP TABLE webpages, t, t_heap, t_heap2;


CREATE TEMP TABLE t(a int);
ALTER TABLE t ADD COLUMN b int;
-- XXX: A better error message would be nice here, but for now this is acceptable.
ALTER TABLE t SET ACCESS METHOD heap;
-- XXX: A better error message would be nice here, but for now this is acceptable.
CREATE INDEX ON t(a);

DROP TABLE t;
CREATE TEMP TABLE t(a int) USING heap;
ALTER TABLE t ADD COLUMN b int;
ALTER TABLE t SET ACCESS METHOD duckdb;

