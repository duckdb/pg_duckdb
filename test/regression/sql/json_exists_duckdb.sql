-- Don't force duckdb execution, all these functions should automatically
-- trigger duckdb execution.
set duckdb.force_duckdb_execution = false;

-- NOTE: In PG17 some of these functions, like JSON_EXISTS and JSON_VALUE, were
-- introduced into Postgres as keywords. This means that to reference those
-- DuckDB functions from Postgres we need specify their "public" schema. We do
-- this for all of these functions just to be sure (except for the one test
-- where we explicitily test correct serialization of the JSON_EXISTS keyword).

-- <JSON_EXISTS>
-- Path exists in a simple JSON object (using a json type)
SELECT public.json_exists('{"a": {"b": 1}}'::json, '$.a.b'); -- Expected: true

-- Path does not exist in a simple JSON object (using JSONB)
CREATE TABLE jsonb_table (j JSONB);
INSERT INTO jsonb_table VALUES ('{"a": {"b": 1}}');
SELECT public.json_exists(j, '$.a.c') FROM jsonb_table; -- Expected: false

-- Without public, but with requiring DuckDB execution. To ensure serialization
-- of JSON_EXISTS keyword is correct.
SELECT json_exists('{"a": {"b": 1}}', '$.a.c') FROM duckdb.query($$ SELECT 1 $$); -- Expected: false

-- BUG: Explicitly specifying the jsonb type causes DuckDB to not understand
-- the query anymore. We should probably serialize this to the json type when
-- we build the query.
SELECT public.json_exists('{"a": {"b": 1}}'::jsonb, '$.a.c');

-- </JSON_EXISTS>
