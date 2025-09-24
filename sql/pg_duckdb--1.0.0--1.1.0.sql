DROP FUNCTION @extschema@.read_parquet(path text, binary_as_string BOOLEAN,
                                                   filename BOOLEAN,
                                                   file_row_number BOOLEAN,
                                                   hive_partitioning BOOLEAN,
                                                   union_by_name BOOLEAN);
DROP FUNCTION @extschema@.read_parquet(path text[], binary_as_string BOOLEAN,
                                                     filename BOOLEAN,
                                                     file_row_number BOOLEAN,
                                                     hive_partitioning BOOLEAN,
                                                     union_by_name BOOLEAN);
-- read_parquet function for single path
CREATE FUNCTION @extschema@.read_parquet(
    path text,
    binary_as_string BOOLEAN DEFAULT FALSE,
    filename BOOLEAN DEFAULT FALSE,
    file_row_number BOOLEAN DEFAULT FALSE,
    hive_partitioning BOOLEAN DEFAULT FALSE,
    hive_types_autocast BOOLEAN DEFAULT TRUE,
    hive_types duckdb.struct DEFAULT NULL,
    encryption_config duckdb.struct DEFAULT NULL,
    union_by_name BOOLEAN DEFAULT FALSE
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_parquet function for array of paths
CREATE FUNCTION @extschema@.read_parquet(
    path text[],
    binary_as_string BOOLEAN DEFAULT FALSE,
    filename BOOLEAN DEFAULT FALSE,
    file_row_number BOOLEAN DEFAULT FALSE,
    hive_partitioning BOOLEAN DEFAULT FALSE,
    hive_types_autocast BOOLEAN DEFAULT TRUE,
    hive_types duckdb.struct DEFAULT NULL,
    encryption_config duckdb.struct DEFAULT NULL,
    union_by_name BOOLEAN DEFAULT FALSE
)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION duckdb.eval(expression text)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE CAST (duckdb.unresolved_type AS duckdb.struct)
    WITH INOUT AS IMPLICIT;

CREATE CAST (duckdb.unresolved_type AS duckdb.map)
    WITH INOUT AS IMPLICIT;

CREATE CAST (duckdb.unresolved_type AS duckdb.union)
    WITH INOUT AS IMPLICIT;
