CREATE FUNCTION @extschema@.delta_scan(path text)
RETURNS SETOF record LANGUAGE 'plpgsql'
SET search_path = pg_catalog, pg_temp
AS
$func$
BEGIN
    RAISE EXCEPTION 'Function `delta_scan(TEXT)` only works with Duckdb execution.';
END;
$func$;
