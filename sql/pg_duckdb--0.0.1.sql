LOAD 'pg_duckdb';

-- We create a duckdb schema to store most of our things. We explicitely
-- don't use CREATE IF EXISTS or the schema key in the control file, so we know
-- for sure that the extension will own the schema and thus non superusers
-- cannot put random things in it, so we can assume it's safe. A few functions
-- we'll put into @extschema@ so that in normal usage they get put into the
-- public schema and are thus more easily usable. This is the case for the
-- read_csv, read_parquet and iceberg functions. It would be sad for usability
-- if people would have to prefix those with duckdb.read_csv
CREATE SCHEMA duckdb;
-- Allow users to see the objects in the duckdb schema. We'll manually revoke rights
-- for the dangerous ones.
GRANT USAGE ON SCHEMA duckdb to PUBLIC;

SET search_path = pg_catalog, pg_temp;

CREATE FUNCTION @extschema@.read_parquet(path text, binary_as_string BOOLEAN DEFAULT FALSE,
                                                   filename BOOLEAN DEFAULT FALSE,
                                                   file_row_number BOOLEAN DEFAULT FALSE,
                                                   hive_partitioning BOOLEAN DEFAULT FALSE,
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION @extschema@.read_parquet(path text[], binary_as_string BOOLEAN DEFAULT FALSE,
                                                     filename BOOLEAN DEFAULT FALSE,
                                                     file_row_number BOOLEAN DEFAULT FALSE,
                                                     hive_partitioning BOOLEAN DEFAULT FALSE,
                                                     union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_parquet(TEXT[])` only works with Duckdb execution.';
END;
$func$;


-- Arguments 'columns' and 'nullstr' are currently not supported for read_csv

CREATE FUNCTION @extschema@.read_csv(path text, all_varchar BOOLEAN DEFAULT FALSE,
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
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_csv(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION @extschema@.read_csv(path text[],  all_varchar BOOLEAN DEFAULT FALSE,
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
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_csv(TEXT[])` only works with Duckdb execution.';
END;
$func$;

-- iceberg_* functions optional parameters are extract from source code;
-- https://github.com/duckdb/duckdb_iceberg/tree/main/src/iceberg_functions

CREATE FUNCTION @extschema@.iceberg_scan(path text, allow_moved_paths BOOLEAN DEFAULT FALSE,
                                                   mode TEXT DEFAULT '',
                                                   metadata_compression_codec TEXT DEFAULT 'none',
                                                   skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                   version TEXT DEFAULT 'version-hint.text',
                                                   version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE TYPE duckdb.iceberg_metadata_record AS (
  manifest_path TEXT,
  manifest_sequence_number NUMERIC,
  manifest_content  TEXT,
  status TEXT,
  content TEXT,
  file_path TEXT
);

CREATE FUNCTION @extschema@.iceberg_metadata(path text, allow_moved_paths BOOLEAN DEFAULT FALSE,
                                                       metadata_compression_codec TEXT DEFAULT 'none',
                                                       skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                       version TEXT DEFAULT 'version-hint.text',
                                                       version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF duckdb.iceberg_metadata_record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_metadata(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE TYPE duckdb.iceberg_snapshots_record AS (
  sequence_number BIGINT,
  snapshot_id BIGINT,
  timestamp_ms TIMESTAMP,
  manifest_list TEXT
);

CREATE FUNCTION @extschema@.iceberg_snapshots(path text, metadata_compression_codec TEXT DEFAULT 'none',
                                                        skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                        version TEXT DEFAULT 'version-hint.text',
                                                        version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF duckdb.iceberg_snapshots_record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `iceberg_snapshots(TEXT)` only works with Duckdb execution.';
END;
$func$;

-- If duckdb.postgres_role is configured let's make sure it's actually created.
-- If people change this setting after installing the extension they are
-- responsible for creating the user themselves. This is especially useful for
-- demo purposes and our pg_regress testing.
DO $$
DECLARE
    role_name text;
BEGIN
    SELECT current_setting('duckdb.postgres_role') INTO role_name;
    IF role_name != '' AND NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles
      WHERE  rolname = role_name) THEN
        EXECUTE 'CREATE ROLE ' || quote_ident(current_setting('duckdb.postgres_role'));
    END IF;
END
$$;


SET search_path TO duckdb, pg_catalog, pg_temp;

CREATE SEQUENCE duckdb.secrets_table_seq START WITH 1 INCREMENT BY 1;
GRANT SELECT ON duckdb.secrets_table_seq TO PUBLIC;
SELECT pg_catalog.setval('duckdb.secrets_table_seq', 1);

set search_path = pg_catalog, pg_temp;
CREATE TABLE duckdb.secrets (
    name TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    type TEXT NOT NULL,
    key_id TEXT NOT NULL,
    secret TEXT NOT NULL,
    region TEXT,
    session_token TEXT,
    endpoint TEXT,
    r2_account_id TEXT,
    use_ssl BOOLEAN DEFAULT true,
    CONSTRAINT type_constraint CHECK (type IN ('S3', 'GCS', 'R2'))
);
SET search_path TO duckdb, pg_catalog, pg_temp;

CREATE FUNCTION duckdb_update_secrets_table_seq()
RETURNS TRIGGER
SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    PERFORM nextval('duckdb.secrets_table_seq');
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;
REVOKE ALL ON FUNCTION duckdb_update_secrets_table_seq() FROM PUBLIC;

CREATE TRIGGER secrets_table_seq_tr AFTER INSERT OR UPDATE OR DELETE ON duckdb.secrets
EXECUTE FUNCTION duckdb_update_secrets_table_seq();

CREATE FUNCTION duckdb_secret_r2_check()
RETURNS TRIGGER
SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    IF NEW.type = 'R2' AND NEW.r2_account_id IS NULL THEN
        Raise Exception '`R2` cloud type secret requires valid `r2_account_id` column value';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;
REVOKE ALL ON FUNCTION duckdb_secret_r2_check() FROM PUBLIC;

CREATE TRIGGER duckdb_secret_r2_tr BEFORE INSERT OR UPDATE ON secrets
FOR EACH ROW EXECUTE PROCEDURE duckdb_secret_r2_check();

-- Extensions

CREATE SEQUENCE extensions_table_seq START WITH 1 INCREMENT BY 1;
SELECT setval('extensions_table_seq', 1);
GRANT SELECT ON extensions_table_seq TO PUBLIC;

CREATE TABLE extensions (
    name TEXT NOT NULL PRIMARY KEY,
    enabled BOOL DEFAULT TRUE
);

CREATE FUNCTION duckdb_update_extensions_table_seq()
RETURNS TRIGGER
SET search_path = pg_catalog, pg_temp
AS
$$
BEGIN
    PERFORM nextval('duckdb.extensions_table_seq');
    RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;
REVOKE ALL ON FUNCTION duckdb_update_extensions_table_seq() FROM PUBLIC;

CREATE TRIGGER extensions_table_seq_tr AFTER INSERT OR UPDATE OR DELETE ON extensions
EXECUTE FUNCTION duckdb_update_extensions_table_seq();

-- The following might seem unnecesasry, but it's needed to know if a dropped
-- table was a DuckDB table or not. See the comments and code in
-- duckdb_drop_table_trigger for details.
CREATE TABLE tables (
    relid regclass PRIMARY KEY,
    duckdb_db TEXT NOT NULL,
    motherduck_catalog_version TEXT
);

REVOKE ALL ON tables FROM PUBLIC;

CREATE FUNCTION install_extension(extension_name TEXT) RETURNS bool
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'install_extension';
REVOKE ALL ON FUNCTION install_extension(TEXT) FROM PUBLIC;

CREATE FUNCTION raw_query(query TEXT) RETURNS void
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_raw_query';
REVOKE ALL ON FUNCTION raw_query(TEXT) FROM PUBLIC;

CREATE FUNCTION cache(object_path TEXT, type TEXT) RETURNS bool
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'cache';
REVOKE ALL ON FUNCTION cache(TEXT, TEXT) FROM PUBLIC;

CREATE FUNCTION duckdb_am_handler(internal)
    RETURNS table_am_handler
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME'
    LANGUAGE C;

CREATE ACCESS METHOD duckdb
    TYPE TABLE
    HANDLER duckdb_am_handler;

CREATE FUNCTION duckdb_drop_trigger() RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_drop_trigger ON sql_drop
    EXECUTE FUNCTION duckdb_drop_trigger();

CREATE FUNCTION duckdb_create_table_trigger() RETURNS event_trigger
    SET search_path = pg_catalog, pg_temp
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_create_table_trigger ON ddl_command_end
    WHEN tag IN ('CREATE TABLE', 'CREATE TABLE AS')
    EXECUTE FUNCTION duckdb_create_table_trigger();

CREATE FUNCTION duckdb_alter_table_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_alter_table_trigger ON ddl_command_end
    WHEN tag IN ('ALTER TABLE')
    EXECUTE FUNCTION duckdb_alter_table_trigger();

-- We explicitly don't set the search_path here in the function definition.
-- Because we actually need the original search_path that was active during the
-- GRANT to resolve the RangeVar using RangeVarGetRelid. We don't need this for
-- any of the other triggers since those don't manually resolve RangeVars, at
-- least not yet. So for those we might as well err on the side of caution and
-- force a safe search_path.
CREATE FUNCTION duckdb_grant_trigger() RETURNS event_trigger
    AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE EVENT TRIGGER duckdb_grant_trigger ON ddl_command_end
    WHEN tag IN ('GRANT')
    EXECUTE FUNCTION duckdb_grant_trigger();

CREATE PROCEDURE force_motherduck_sync(drop_with_cascade BOOLEAN DEFAULT false)
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME';

CREATE FUNCTION recycle_ddb() RETURNS void
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_recycle_ddb';
REVOKE ALL ON FUNCTION recycle_ddb() FROM PUBLIC;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname LIKE 'ddb$%') THEN
        RAISE 'pg_duckdb can only be installed if there are no schemas with a ddb$ prefix';
    END IF;
END
$$;

RESET search_path;
