-- Regression test for result-type handling in the extended query protocol.
--
-- PostgreSQL's parser and DuckDB can assign different types to the same result
-- column. For example ROUND(AVG(bigint) / 1024.0, 2) is NUMERIC according to
-- the Postgres parser but DOUBLE according to DuckDB. The Postgres parser type
-- is what gets frozen into the cached plan's result descriptor (and sent as the
-- RowDescription) in the extended / cached-plan path (PREPARE/EXECUTE, JDBC,
-- ...). pg_duckdb must therefore produce values of the parser-declared type, so
-- that the cached-plan path returns the same correct value as the simple
-- protocol instead of reinterpreting the bytes (which previously yielded
-- garbage such as 0.000000000000000000000000000000).
SET duckdb.force_execution = true;

CREATE TABLE metrics(id int, vol bigint);
INSERT INTO metrics SELECT g, (g * 1000)::bigint FROM generate_series(1, 100) g;

-- Simple protocol: baseline correct value (Postgres parser type is numeric).
SELECT ROUND(AVG(vol) / 1024.0, 2) AS v FROM metrics;

-- The cached plan's result type is numeric (this is what the extended protocol
-- uses for the row description / output).
PREPARE pbug AS SELECT ROUND(AVG(vol) / 1024.0, 2) AS v FROM metrics;
SELECT result_types FROM pg_prepared_statements WHERE name = 'pbug';

-- Cached-plan path: must match the simple-protocol value, not corrupt it.
EXECUTE pbug;
EXECUTE pbug;

-- Parameterized + cached plan: parameter in the filter, numeric result column.
PREPARE pbug2(int) AS SELECT ROUND(AVG(vol) / 1024.0, 2) AS v FROM metrics WHERE id > $1;
EXECUTE pbug2(0);
EXECUTE pbug2(0);

-- Bare aggregate: Postgres types AVG(bigint) as numeric, DuckDB as double. This
-- exercises the double -> numeric coercion directly (no surrounding ROUND).
PREPARE pavg AS SELECT AVG(vol) AS v FROM metrics;
SELECT result_types FROM pg_prepared_statements WHERE name = 'pavg';
EXECUTE pavg;

-- Postgres numeric where DuckDB also produces a numeric (DECIMAL): the oids
-- already match so we keep the DuckDB type; the value must still pass through
-- the cached-plan path unchanged.
PREPARE pdec AS SELECT MIN(vol) + 0.5 AS v FROM metrics;
SELECT result_types FROM pg_prepared_statements WHERE name = 'pdec';
EXECUTE pdec;

-- Control: when Postgres and DuckDB agree on the type (double), the value is
-- already correct and stays unchanged.
PREPARE pok AS SELECT (AVG(vol) / 1024.0)::float8 AS v FROM metrics;
SELECT result_types FROM pg_prepared_statements WHERE name = 'pok';
EXECUTE pok;

-- Control: integer aggregate, both agree on the type.
PREPARE pcount AS SELECT count(*) AS c FROM metrics;
EXECUTE pcount;

DEALLOCATE ALL;
DROP TABLE metrics;
