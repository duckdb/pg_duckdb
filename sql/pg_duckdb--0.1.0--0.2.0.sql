CREATE FUNCTION @extschema@.delta_scan(path text)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `delta_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION @extschema@.read_json(path text, auto_detect BOOLEAN DEFAULT FALSE,
                                                 compression VARCHAR DEFAULT 'auto',
                                                 dateformat VARCHAR DEFAULT 'iso',
                                                 format VARCHAR DEFAULT 'array',
                                                 ignore_errors BOOLEAN DEFAULT FALSE,
                                                 maximum_depth BIGINT DEFAULT -1,
                                                 maximum_object_size INT DEFAULT 16777216,
                                                 records VARCHAR DEFAULT 'records',
                                                 sample_size BIGINT DEFAULT 20480,
                                                 timestampformat VARCHAR DEFAULT 'iso',
                                                 union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_json(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE FUNCTION @extschema@.read_json(path text[], auto_detect BOOLEAN DEFAULT FALSE,
                                                   compression VARCHAR DEFAULT 'auto',
                                                   dateformat VARCHAR DEFAULT 'iso',
                                                   format VARCHAR DEFAULT 'array',
                                                   ignore_errors BOOLEAN DEFAULT FALSE,
                                                   maximum_depth BIGINT DEFAULT -1,
                                                   maximum_object_size INT DEFAULT 16777216,
                                                   records VARCHAR DEFAULT 'records',
                                                   sample_size BIGINT DEFAULT 20480,
                                                   timestampformat VARCHAR DEFAULT 'iso',
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `read_json(TEXT[])` only works with Duckdb execution.';
END;
$func$;
