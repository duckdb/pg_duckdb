CREATE FUNCTION @extschema@.approx_count_distinct_sfunc(bigint, anyelement)
RETURNS bigint LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Aggregate `approx_count_distinct(ANYELEMENT)` only works with Duckdb execution.';
END;
$func$;

CREATE AGGREGATE @extschema@.approx_count_distinct(anyelement)
(
    sfunc = @extschema@.approx_count_distinct_sfunc,
    stype = bigint,
    initcond = 0
);

CREATE DOMAIN pg_catalog.blob AS bytea;
COMMENT ON DOMAIN pg_catalog.blob IS 'The DuckDB BLOB alias for BYTEA';

-- json_exists
CREATE FUNCTION @extschema@.json_exists(json json , path VARCHAR)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `jsonb_exists(json, VARCHAR)` only works with Duckdb execution.';
END;
$func$;

-- json_extract
CREATE FUNCTION @extschema@.json_extract(json json, path VARCHAR)
RETURNS JSON LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_extract with path list
CREATE FUNCTION @extschema@.json_extract(json json, path VARCHAR[])
RETURNS JSON LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_extract_string
CREATE FUNCTION @extschema@.json_extract_string(json json, path VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract_string(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_extract_string
CREATE FUNCTION @extschema@.json_extract_string(json json, path VARCHAR[])
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract_string(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_value
CREATE FUNCTION @extschema@.json_value(json json, path VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_value(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_array_length
CREATE FUNCTION @extschema@.json_array_length(json json, path_input VARCHAR)
RETURNS integer LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_array_length(json)` only works with DuckDB execution.';
END;
$func$;

-- json_contains
CREATE FUNCTION @extschema@.json_contains(json_haystack json, json_needle json)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_contains(json, json)` only works with DuckDB execution.';
END;
$func$;

-- json_keys
CREATE FUNCTION @extschema@.json_keys(json json, path VARCHAR DEFAULT NULL)
RETURNS SETOF VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_keys(json, [VARCHAR])` only works with DuckDB execution.';
END;
$func$;

-- json_structure
CREATE FUNCTION @extschema@.json_structure(json json)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_structure(json)` only works with DuckDB execution.';
END;
$func$;

-- json_type
CREATE FUNCTION @extschema@.json_type(json json, path VARCHAR[] DEFAULT NULL)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_type(json)` only works with DuckDB execution.';
END;
$func$;

-- json_valid
CREATE FUNCTION @extschema@.json_valid(json json)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_valid(json)` only works with DuckDB execution.';
END;
$func$;

-- json
CREATE FUNCTION @extschema@.json(json json)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json(json)` only works with DuckDB execution.';
END;
$func$;

-- json_group_array
CREATE FUNCTION @extschema@.json_group_array(any_element ANYELEMENT)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_group_array(json)` only works with DuckDB execution.';
END;
$func$;

-- json_group_object
CREATE FUNCTION @extschema@.json_group_object(key ANYELEMENT, value ANYELEMENT)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_group_object(ANYELEMENT, ANYELEMENT)` only works with DuckDB execution.';
END;
$func$;

-- json_group_structure
CREATE FUNCTION @extschema@.json_group_structure(json json)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_group_structure(json)` only works with DuckDB execution.';
END;
$func$;

-- json_transform
CREATE FUNCTION @extschema@.json_transform(json json, structure VARCHAR)
RETURNS json LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_transform(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- from_json
CREATE FUNCTION @extschema@.from_json(json json, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `from_json(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_transform_strict
CREATE FUNCTION @extschema@.json_transform_strict(json json, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_transform_strict(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;


-- from_json_strict
CREATE FUNCTION @extschema@.from_json_strict(json json, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `from_json_strict(json, VARCHAR)` only works with DuckDB execution.';
END;
$func$;


