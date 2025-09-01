-- Add MAP functions support
-- Extract value from map using key
CREATE FUNCTION @extschema@.map_extract(map_col duckdb.map, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_extract(map_col duckdb.unresolved_type, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Get all keys from map
CREATE FUNCTION @extschema@.map_keys(map_col duckdb.map)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_keys(map_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Get all values from map
CREATE FUNCTION @extschema@.map_values(map_col duckdb.map)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_values(map_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Get map size
CREATE FUNCTION @extschema@.cardinality(map_col duckdb.map)
RETURNS integer AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.cardinality(map_col duckdb.unresolved_type)
RETURNS integer AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Get element at key (alias for map_extract)
CREATE FUNCTION @extschema@.element_at(map_col duckdb.map, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.element_at(map_col duckdb.unresolved_type, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.element_at(map_col duckdb.map, key integer)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.element_at(map_col duckdb.unresolved_type, key integer)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Merge multiple maps
CREATE FUNCTION @extschema@.map_concat(map_col duckdb.map, map_col2 duckdb.map)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_concat(map_col duckdb.unresolved_type, map_col2 duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Check if map contains key
CREATE FUNCTION @extschema@.map_contains(map_col duckdb.map, key text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_contains(map_col duckdb.unresolved_type, key text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_contains(map_col duckdb.map, key integer)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_contains(map_col duckdb.unresolved_type, key integer)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Check if map contains key-value pair
CREATE FUNCTION @extschema@.map_contains_entry(map_col duckdb.map, key text, value text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_contains_entry(map_col duckdb.unresolved_type, key text, value text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Check if map contains value
CREATE FUNCTION @extschema@.map_contains_value(map_col duckdb.map, value text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_contains_value(map_col duckdb.unresolved_type, value text)
RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Get all key-value pairs as structs
CREATE FUNCTION @extschema@.map_entries(map_col duckdb.map)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_entries(map_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Extract single value from map (not as list)
CREATE FUNCTION @extschema@.map_extract_value(map_col duckdb.map, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_extract_value(map_col duckdb.unresolved_type, key text)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_extract_value(map_col duckdb.map, key integer)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
CREATE FUNCTION @extschema@.map_extract_value(map_col duckdb.unresolved_type, key integer)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;

-- Create map from array of struct(k, v)
CREATE FUNCTION @extschema@.map_from_entries(entries duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_only_function' LANGUAGE C;
