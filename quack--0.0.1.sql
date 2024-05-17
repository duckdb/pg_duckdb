LOAD 'quack';

CREATE OR REPLACE FUNCTION read_parquet(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
  RETURN QUERY EXECUTE 'SELECT 1';
END;
$func$;
