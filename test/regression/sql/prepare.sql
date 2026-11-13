-- the view should return the empty
SELECT name, statement, parameter_types FROM pg_prepared_statements;

CREATE TABLE copy_database(datname text, datistemplate boolean, datallowconn boolean);
INSERT INTO copy_database SELECT datname, datistemplate, datallowconn FROM pg_database;
-- parameterized queries
PREPARE q1(text) AS
	SELECT datname, datistemplate, datallowconn
	FROM copy_database WHERE datname = $1;

EXECUTE q1('postgres');

-- q1
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

CREATE TABLE ta(a int);
INSERT INTO ta(a) SELECT * FROM generate_series(1, 1000);
SELECT COUNT(*) FROM ta;
PREPARE q2 AS SELECT COUNT(*) FROM ta;
EXECUTE q2;

-- q1 q2
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

PREPARE q3(int) AS
	SELECT * FROM ta WHERE a = $1;

CREATE TEMPORARY TABLE q3_prep_results AS EXECUTE q3(200);
SELECT * FROM q3_prep_results;

-- q1 q2 q3
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

CREATE TABLE tb (a int DEFAULT 1, b int, c varchar DEFAULT 'pg_duckdb');
INSERT INTO tb(b) VALUES(2);
PREPARE q4(int, varchar) AS
	SELECT b FROM tb WHERE a = $1 AND c = $2;
EXECUTE q4(1, 'pg_duckdb');
EXECUTE q4(1, 'pg_duckdb');

-- q1 q2 q3 q4
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

TRUNCATE tb;
INSERT INTO tb VALUES(10, 10, 'test');
INSERT INTO tb VALUES(100, 100, 'pg_duckdb');
PREPARE q5(int, varchar) AS
	SELECT b FROM tb WHERE a = $1 AND b = $1 AND c = $2;
EXECUTE q5(10, 'test');
EXECUTE q5(100, 'pg_duckdb');

-- q1 q2 q3 q4 q5
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

PREPARE q6(int, varchar) AS
	SELECT b FROM tb WHERE c = $2;
EXECUTE q6(10, 'test');

-- q1 q2 q3 q4 q5 q6
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

PREPARE q7(int, varchar) AS
	SELECT b FROM tb WHERE a = $1;
EXECUTE q7(100, 'pg_duckdb');

-- q1 q2 q3 q4 q5 q6 q7
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

-- test DEALLOCATE ALL;
DEALLOCATE ALL;
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

DROP TABLE copy_database, ta, tb;

-- Prepared statements can't return columns whose type is only known when
-- DuckDB executes the query (#1018)
PREPARE duckdb_columns AS SELECT r['upper'], r['LOWER'] FROM read_parquet('../../data/uppercase.parquet') r;
PREPARE duckdb_star AS SELECT * FROM read_parquet('../../data/uppercase.parquet');
-- The prepared statements were not kept
EXECUTE duckdb_columns;
SELECT name FROM pg_prepared_statements;
-- Casting the columns works
PREPARE duckdb_cast AS SELECT r['upper']::int AS up, r['LOWER']::int AS low FROM read_parquet('../../data/uppercase.parquet') r;
EXECUTE duckdb_cast;
DEALLOCATE ALL;
