-- For this test we duckdb set execution to false
SET duckdb.force_execution = false;
CREATE TABLE t(a int);
INSERT INTO t VALUES (1);

CREATE TEMP TABLE t_ddb(a int) USING duckdb;
INSERT INTO t_ddb VALUES (1);

BEGIN;
SELECT * FROM t_ddb;
INSERT INTO t_ddb VALUES (2);
SELECT * FROM t_ddb ORDER BY a;
ROLLBACK;

SELECT * FROM t_ddb;

-- Writing to PG and DDB tables in the same transaction is not supported. We
-- fail early for simple DML (no matter the order).
BEGIN;
INSERT INTO t_ddb VALUES (2);
INSERT INTO t VALUES (2);
ROLLBACK;

BEGIN;
INSERT INTO t VALUES (2);
INSERT INTO t_ddb VALUES (2);
ROLLBACK;

-- And for other writes that are not easy to detect, such as CREATE TABLE, we
-- fail on COMMIT.
BEGIN;
INSERT INTO t_ddb VALUES (2);
CREATE TABLE t2(a int);
COMMIT;

-- Savepoints in PG-only transactions should still work
BEGIN;
INSERT INTO t VALUES (2);
SAVEPOINT my_savepoint;
INSERT INTO t VALUES (3);
ROLLBACK TO SAVEPOINT my_savepoint;
COMMIT;

-- But savepoints are not allowed in DuckDB transactions
BEGIN;
INSERT INTO t_ddb VALUES (2);
SAVEPOINT my_savepoint;
ROLLBACK;;

-- Also not already started ones
BEGIN;
SAVEPOINT my_savepoint;
INSERT INTO t_ddb VALUES (2);
ROLLBACK;;

-- Unless the subtransaction is already completed
BEGIN;
SAVEPOINT my_savepoint;
SELECT count(*) FROM t;
RELEASE SAVEPOINT my_savepoint;
INSERT INTO t_ddb VALUES (2);
COMMIT;

-- Statements in functions should be run inside a single transaction. So a
-- failure later in the function should roll back.
CREATE OR REPLACE FUNCTION f(fail boolean) RETURNS void
    LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT
    AS
$$
BEGIN
INSERT INTO t_ddb VALUES (8);
IF fail THEN
    RAISE EXCEPTION 'fail';
END IF;
END;
$$;

-- After executing the function the table should not contain the value 8,
-- because that change was rolled back
SELECT * FROM f(true);
SELECT * FROM t_ddb ORDER BY a;

-- But if the function succeeds we should see the new value
SELECT * FROM f(false);
SELECT * FROM t_ddb ORDER BY a;

-- DuckDB DDL in transactions is not allowed for now
BEGIN;
    CREATE TABLE t_ddb2(a int) USING duckdb;
END;

-- Neither is DDL in functions

CREATE OR REPLACE FUNCTION f2() RETURNS void
    LANGUAGE plpgsql
    RETURNS NULL ON NULL INPUT
    AS
$$
BEGIN
    CREATE TABLE t_ddb2(a int) USING duckdb;
END;
$$;

SELECT * FROM f2();

TRUNCATE t_ddb;
INSERT INTO t_ddb VALUES (1);

BEGIN;
DECLARE c SCROLL CURSOR FOR SELECT a FROM t_ddb;
FETCH NEXT FROM c;
FETCH NEXT FROM c;
FETCH PRIOR FROM c;
COMMIT;

DROP FUNCTION f, f2;
