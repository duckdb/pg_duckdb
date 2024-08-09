LOAD 'pg_duckdb';

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

CREATE OR REPLACE FUNCTION iceberg_scan(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE SCHEMA duckdb;
SET search_path TO duckdb;

CREATE TABLE secrets (
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    secret TEXT NOT NULL,
    region TEXT,
    endpoint TEXT,
    r2_account_id TEXT,
    CONSTRAINT type_constraint CHECK (type IN ('S3', 'GCS', 'R2'))
);

CREATE OR REPLACE FUNCTION duckdb_secret_r2_check()
RETURNS TRIGGER AS
$$
BEGIN
    IF NEW.type = 'R2' AND NEW.r2_account_id IS NULL THEN
        Raise Exception '`R2` cloud type secret requires valid `r2_account_id` column value';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;

CREATE TRIGGER duckdb_secret_r2_tr BEFORE INSERT OR UPDATE ON secrets
FOR EACH ROW EXECUTE PROCEDURE duckdb_secret_r2_check();

CREATE TABLE extensions (
    name TEXT NOT NULL,
    enabled BOOL DEFAULT TRUE
);

CREATE TABLE tables (
    relid regclass
);

CREATE OR REPLACE FUNCTION install_extension(extension_name TEXT) RETURNS bool
    LANGUAGE C AS 'MODULE_PATHNAME', 'install_extension';

CREATE OR REPLACE FUNCTION query(query TEXT) RETURNS void
    LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_query';

CREATE FUNCTION duckdb_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD duckdb
    TYPE TABLE
    HANDLER duckdb_am_handler;

CREATE FUNCTION duckdb_drop_table_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_drop_table_trigger ON sql_drop
    EXECUTE FUNCTION duckdb_drop_table_trigger();

CREATE FUNCTION duckdb_create_table_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_create_table_trigger ON ddl_command_end
    WHEN tag IN ('CREATE TABLE', 'CREATE TABLE AS')
    EXECUTE FUNCTION duckdb_create_table_trigger();

DO $$
BEGIN
    RAISE WARNING 'To actually execute queries using DuckDB you need to run "SET duckdb.execution TO true;"';
END
$$;

RESET search_path;
