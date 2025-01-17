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
CREATE FUNCTION @extschema@.json_exists(json VARCHAR , path VARCHAR)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `jsonb_exists(VARCHAR, VARCHAR)` only works with Duckdb execution.';
END;
$func$;


-- json_extract
CREATE FUNCTION @extschema@.json_extract(json VARCHAR, path VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_extract with path list
CREATE FUNCTION @extschema@.json_extract(json VARCHAR, path VARCHAR[])
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;
-- json_extract_string
CREATE FUNCTION @extschema@.json_extract_string(json VARCHAR, path VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_extract_string(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_value
CREATE FUNCTION @extschema@.json_value(json VARCHAR, path VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_value(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_array_length
CREATE FUNCTION @extschema@.json_array_length(json VARCHAR)
RETURNS integer LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_array_length(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_contains
CREATE FUNCTION @extschema@.json_contains(json_haystack VARCHAR, json_needle VARCHAR)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_contains(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_keys
CREATE FUNCTION @extschema@.json_keys(json VARCHAR, path VARCHAR DEFAULT NULL)
RETURNS SETOF VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_keys(VARCHAR, [VARCHAR])` only works with DuckDB execution.';
END;
$func$;

-- json_structure
CREATE FUNCTION @extschema@.json_structure(json VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_structure(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_type
CREATE FUNCTION @extschema@.json_type(json VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_type(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_valid
CREATE FUNCTION @extschema@.json_valid(json VARCHAR)
RETURNS boolean LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_valid(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json
CREATE FUNCTION @extschema@.json(json VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_group_array
CREATE FUNCTION @extschema@.json_group_array(any_element ANYELEMENT)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_group_array(ANYELEMENT)` only works with DuckDB execution.';
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
CREATE FUNCTION @extschema@.json_group_structure(json VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_group_structure(VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_transform
CREATE FUNCTION @extschema@.json_transform(json VARCHAR, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_transform(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- from_json
CREATE FUNCTION @extschema@.from_json(json VARCHAR, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `from_json(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;

-- json_transform_strict
CREATE FUNCTION @extschema@.json_transform_strict(json VARCHAR, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `json_transform_strict(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;


-- from_json_strict
CREATE FUNCTION @extschema@.from_json_strict(json VARCHAR, structure VARCHAR)
RETURNS VARCHAR LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `from_json_strict(VARCHAR, VARCHAR)` only works with DuckDB execution.';
END;
$func$;


