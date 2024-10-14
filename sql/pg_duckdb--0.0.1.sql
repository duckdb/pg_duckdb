LOAD 'pg_duckdb';

CREATE OR REPLACE FUNCTION read_parquet(path text, binary_as_string BOOLEAN DEFAULT FALSE,
                                                   filename BOOLEAN DEFAULT FALSE,
                                                   file_row_number BOOLEAN DEFAULT FALSE,
                                                   hive_partitioning BOOLEAN DEFAULT FALSE,
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_parquet(path text[], binary_as_string BOOLEAN DEFAULT FALSE,
                                                     filename BOOLEAN DEFAULT FALSE,
                                                     file_row_number BOOLEAN DEFAULT FALSE,
                                                     hive_partitioning BOOLEAN DEFAULT FALSE,
                                                     union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT[])` only works with Duckdb execution.';
END;
$func$;


-- Arguments 'columns' and 'nullstr' are currently not supported for read_csv

CREATE OR REPLACE FUNCTION read_csv(path text, all_varchar BOOLEAN DEFAULT FALSE,
                                               allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                               auto_detect BOOLEAN DEFAULT TRUE,
                                               auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               compression VARCHAR DEFAULT 'auto',
                                               dateformat VARCHAR DEFAULT '',
                                               decimal_separator VARCHAR DEFAULT '.',
                                               delim VARCHAR DEFAULT ',',
                                               escape VARCHAR DEFAULT '"',
                                               filename BOOLEAN DEFAULT FALSE,
                                               force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               header BOOLEAN DEFAULT FALSE,
                                               hive_partitioning BOOLEAN DEFAULT FALSE,
                                               ignore_errors BOOLEAN DEFAULT FALSE,
                                               max_line_size BIGINT DEFAULT 2097152,
                                               names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               new_line VARCHAR DEFAULT '',
                                               normalize_names BOOLEAN DEFAULT FALSE,
                                               null_padding BOOLEAN DEFAULT FALSE,
                                               nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               parallel BOOLEAN DEFAULT FALSE,
                                               quote VARCHAR DEFAULT '"',
                                               sample_size BIGINT DEFAULT 20480,
                                               sep VARCHAR DEFAULT ',',
                                               skip BIGINT DEFAULT 0,
                                               timestampformat VARCHAR DEFAULT '',
                                               types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_csv(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_csv(path text[],  all_varchar BOOLEAN DEFAULT FALSE,
                                                  allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                                  auto_detect BOOLEAN DEFAULT TRUE,
                                                  auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  compression VARCHAR DEFAULT 'auto',
                                                  dateformat VARCHAR DEFAULT '',
                                                  decimal_separator VARCHAR DEFAULT '.',
                                                  delim VARCHAR DEFAULT ',',
                                                  escape VARCHAR DEFAULT '"',
                                                  filename BOOLEAN DEFAULT FALSE,
                                                  force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  header BOOLEAN DEFAULT FALSE,
                                                  hive_partitioning BOOLEAN DEFAULT FALSE,
                                                  ignore_errors BOOLEAN DEFAULT FALSE,
                                                  max_line_size BIGINT DEFAULT 2097152,
                                                  names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  new_line VARCHAR DEFAULT '',
                                                  normalize_names BOOLEAN DEFAULT FALSE,
                                                  null_padding BOOLEAN DEFAULT FALSE,
                                                  nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  parallel BOOLEAN DEFAULT FALSE,
                                                  quote VARCHAR DEFAULT '"',
                                                  sample_size BIGINT DEFAULT 20480,
                                                  sep VARCHAR DEFAULT ',',
                                                  skip BIGINT DEFAULT 0,
                                                  timestampformat VARCHAR DEFAULT '',
                                                  types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  union_by_name BOOLEAN DEFAULT FALSE)
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

CREATE SEQUENCE secrets_table_seq START WITH 1 INCREMENT BY 1;
SELECT setval('secrets_table_seq', 1);

CREATE TABLE secrets (
    type TEXT NOT NULL,
    id TEXT NOT NULL,
    secret TEXT NOT NULL,
    region TEXT,
    session_token TEXT,
    endpoint TEXT,
    r2_account_id TEXT,
    use_ssl BOOLEAN DEFAULT true,
    CONSTRAINT type_constraint CHECK (type IN ('S3', 'GCS', 'R2'))
);

CREATE OR REPLACE FUNCTION duckdb_update_secrets_table_seq()
RETURNS TRIGGER AS
$$
BEGIN
    PERFORM nextval('duckdb.secrets_table_seq');
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;

CREATE TRIGGER secrets_table_seq_tr AFTER INSERT OR UPDATE OR DELETE ON secrets
EXECUTE FUNCTION duckdb_update_secrets_table_seq();

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

-- The following might seem unnecesasry, but it's needed to know if a dropped
-- table was a DuckDB table or not. See the comments and code in
-- duckdb_drop_table_trigger for details.
CREATE TABLE tables (
    relid regclass PRIMARY KEY,
    duckdb_db TEXT NOT NULL,
    motherduck_catalog_version TEXT
);

REVOKE ALL ON tables FROM PUBLIC;

CREATE OR REPLACE FUNCTION install_extension(extension_name TEXT) RETURNS bool
    LANGUAGE C AS 'MODULE_PATHNAME', 'install_extension';

CREATE OR REPLACE FUNCTION raw_query(query TEXT) RETURNS void
    LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_raw_query';

CREATE OR REPLACE FUNCTION cache(object_path TEXT, type TEXT) RETURNS bool
    LANGUAGE C AS 'MODULE_PATHNAME', 'cache';

CREATE FUNCTION duckdb_am_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE ACCESS METHOD duckdb
    TYPE TABLE
    HANDLER duckdb_am_handler;

CREATE FUNCTION duckdb_drop_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_drop_trigger ON sql_drop
    EXECUTE FUNCTION duckdb_drop_trigger();

CREATE FUNCTION duckdb_create_table_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_create_table_trigger ON ddl_command_end
    WHEN tag IN ('CREATE TABLE', 'CREATE TABLE AS')
    EXECUTE FUNCTION duckdb_create_table_trigger();

CREATE FUNCTION duckdb_alter_table_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_alter_table_trigger ON ddl_command_end
    WHEN tag IN ('ALTER TABLE')
    EXECUTE FUNCTION duckdb_alter_table_trigger();

CREATE FUNCTION duckdb_grant_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_grant_trigger ON ddl_command_end
    WHEN tag IN ('GRANT')
    EXECUTE FUNCTION duckdb_grant_trigger();

CREATE OR REPLACE PROCEDURE force_motherduck_sync(drop_with_cascade BOOLEAN DEFAULT false)
    LANGUAGE C AS 'MODULE_PATHNAME';

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname LIKE 'ddb$%') THEN
        RAISE 'pg_duckdb can only be installed if there are no schemas with a ddb$ prefix';
    END IF;
END
$$;

DO $$
BEGIN
    RAISE WARNING 'To actually execute queries using DuckDB you need to run "SET duckdb.execution TO true;"';
END
$$;

RESET search_path;
