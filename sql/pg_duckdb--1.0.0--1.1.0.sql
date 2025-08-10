-- Add MAP functions support
-- These functions are already implemented in DuckDB, we just need to expose them in Postgres

-- map_extract function - extracts a value from a map using a key
CREATE FUNCTION @extschema@.map_extract(map_col duckdb.map, key text)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- map_extract function with unresolved_type for flexibility
CREATE FUNCTION @extschema@.map_extract(map_col duckdb.unresolved_type, key text)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- map_keys function - returns all keys from a map as a list
CREATE FUNCTION @extschema@.map_keys(map_col duckdb.map)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- map_keys function with unresolved_type for flexibility
CREATE FUNCTION @extschema@.map_keys(map_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- map_values function - returns all values from a map as a list
CREATE FUNCTION @extschema@.map_values(map_col duckdb.map)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- map_values function with unresolved_type for flexibility
CREATE FUNCTION @extschema@.map_values(map_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;
