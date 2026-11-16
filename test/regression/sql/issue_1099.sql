-- Issue #1099: a DuckDB temp table that was dropped behind our back (via
-- raw_query) should not crash the backend during ON COMMIT DELETE ROWS.
CREATE TEMP TABLE alter_test(id INT) USING duckdb ON COMMIT DELETE ROWS;
INSERT INTO alter_test VALUES (1);
SELECT duckdb.raw_query('DROP TABLE pg_temp.main.alter_test');
CREATE TEMP TABLE t_ddb(a int) USING duckdb ON COMMIT DELETE ROWS;
-- Dropping the orphaned Postgres table should still work
DROP TABLE alter_test;
CREATE TEMP TABLE t_ddb2(a int) USING duckdb ON COMMIT DELETE ROWS;
INSERT INTO t_ddb2 VALUES (1);
SELECT * FROM t_ddb2;
DROP TABLE t_ddb2;
