CREATE FUNCTION test_escape_uri(
    input TEXT
)
RETURNS TEXT
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'pg_duckdb', 'pgduckdb_test_escape_uri';

SELECT test_escape_uri('https://duckdb.org');

SELECT test_escape_uri('https://duckdb.org/with space');

SELECT test_escape_uri('foo $ bar # baz / qux');
