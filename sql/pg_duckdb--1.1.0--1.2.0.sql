-- read_vortex function for single path
CREATE FUNCTION @extschema@.read_vortex(path text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_vortex function for array of paths
CREATE FUNCTION @extschema@.read_vortex(path text[])
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_text function for single path
CREATE FUNCTION @extschema@.read_text(path text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_text function for array of paths
CREATE FUNCTION @extschema@.read_text(path text[])
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_blob function for single path
CREATE FUNCTION @extschema@.read_blob(path text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_blob function for array of paths
CREATE FUNCTION @extschema@.read_blob(path text[])
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- iceberg_snapshots returns parent_id, operation and summary since DuckDB v2.0
DROP FUNCTION @extschema@.iceberg_snapshots(path text, metadata_compression_codec TEXT, skip_schema_inference BOOLEAN,
                                            version TEXT, version_name_format TEXT);
DROP TYPE duckdb.iceberg_snapshots_record;

CREATE TYPE duckdb.iceberg_snapshots_record AS (
  sequence_number BIGINT,
  snapshot_id BIGINT,
  parent_id BIGINT,
  timestamp_ms TIMESTAMP,
  manifest_list TEXT,
  operation TEXT,
  summary duckdb.map
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
