-- Issue #1099: a DuckDB temp table that was dropped behind our back (via
-- raw_query) should not crash the backend during ON COMMIT DELETE ROWS.
CREATE TEMP TABLE alter_test(id INT) USING duckdb ON COMMIT DELETE ROWS;
INSERT INTO alter_test VALUES (1);
SELECT duckdb.raw_query('DROP TABLE pg_temp.main.alter_test');
CREATE TEMP TABLE t_ddb(a int) USING duckdb ON COMMIT DELETE ROWS;
-- Dropping the orphaned Postgres table should still work
DROP TABLE alter_test;
-- The failed commit should also have rolled back the DuckDB side of the
-- CREATE, so creating a table with the same name works.
CREATE TEMP TABLE t_ddb(a int) USING duckdb ON COMMIT DELETE ROWS;
INSERT INTO t_ddb VALUES (1);
SELECT * FROM t_ddb;
-- Rows are deleted at commit of a transaction block
BEGIN;
INSERT INTO t_ddb VALUES (2);
SELECT * FROM t_ddb;
COMMIT;
SELECT * FROM t_ddb;
-- Rows of ON COMMIT PRESERVE ROWS tables are kept
CREATE TEMP TABLE t_keep(a int) USING duckdb;
BEGIN;
INSERT INTO t_keep VALUES (1);
INSERT INTO t_ddb VALUES (3);
COMMIT;
SELECT * FROM t_keep;
SELECT * FROM t_ddb;
-- An explicit TRUNCATE of a table created in the same transaction still works
BEGIN;
CREATE TEMP TABLE t_new(a int) USING duckdb ON COMMIT DELETE ROWS;
INSERT INTO t_new VALUES (1);
TRUNCATE t_new;
INSERT INTO t_new VALUES (2);
SELECT * FROM t_new;
COMMIT;
SELECT * FROM t_new;
-- A failing truncate at commit rolls back the whole transaction
BEGIN;
INSERT INTO t_keep VALUES (2);
SELECT duckdb.raw_query('DROP TABLE pg_temp.main.t_ddb');
COMMIT;
SELECT * FROM t_keep;
DROP TABLE t_ddb, t_keep, t_new;
