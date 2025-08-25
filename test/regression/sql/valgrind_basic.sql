CREATE EXTENSION pg_duckdb;

SELECT * FROM duckdb.query('SELECT 42 as answer');

SELECT * FROM duckdb.query('SELECT generate_series(1, 10) as num');

SELECT * FROM duckdb.query('SELECT ''Hello, Valgrind!'' as message');
