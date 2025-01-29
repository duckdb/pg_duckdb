CREATE TABLE a(b int);
CREATE TABLE "A"(c int);
INSERT INTO a VALUES(1);
INSERT INTO "A" VALUES(2);

-- BUG: This fails because DuckDB considers the aliasses "a" and "A" to be
-- equivalent. This is something that can probably be fixed most easily in
-- DuckDB by having a mode in which aliases are case-sensitive.
SELECT * FROM a, "A";
-- Luckily there's an easy workaround for that
SELECT * FROM a, "A" as a2;

CREATE SCHEMA b;
CREATE SCHEMA "B";

CREATE TABLE b.a(d int);
CREATE TABLE "B".a(e int);

INSERT INTO b.a VALUES(3);
INSERT INTO "B".a VALUES(4);

-- This actually succeeds, in contrast to the similar query above. because the
-- tables have the same casing in Postgres too, so Postgres will give them
-- unique automatic aliases.
SELECT * FROM b.a, "B".a;
-- We can make it fail in the same way with an explicit alias though.
SELECT * FROM b.a, "B".a as "A";
-- With different explicit aliases it should obviously succeed too..
SELECT * FROM b.a, "B".a as a2;

-- Supports casing in column names
select r['UPPER'], r['lower'] from read_parquet('/home/jelte/work/pg_duckdb/test/regression/data/uppercase.parquet') r;

-- DuckDB is case insensitive though. So this should work too. But it will
-- still return the original casing.
select r['upper'], r['LOWER'] from read_parquet('/home/jelte/work/pg_duckdb/test/regression/data/uppercase.parquet') r;

-- You can change the casing by using aliasses (issue #564)
select r['UPPER'] as upper, r['lower'] as "LOWER" from read_parquet('/home/jelte/work/pg_duckdb/test/regression/data/uppercase.parquet') r;

set client_min_messages TO WARNING;
DROP TABLE a, "A";
DROP SCHEMA b, "B" CASCADE;

