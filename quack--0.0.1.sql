LOAD 'quack';

CREATE OR REPLACE FUNCTION read_parquet(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_parquet(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_parquet(path text[])
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_parquet(TEXT[])` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_csv(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_csv(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_csv(path text[])
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_csv(TEXT[])` only works with Duckdb execution.';
END;
$func$;
