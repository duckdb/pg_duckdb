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

-- <JSON_EXTRACT>
-- Basic JSON Extraction (using a string literal)
SELECT public.json_extract('{"key": "value"}', '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction (from a duckdb.query result, i.e. a value of type duckdb.unresolved_type)
SELECT public.json_extract(r['data'], '$.a.b.c') AS result FROM duckdb.query($$ SELECT '{"a": {"b": {"c": 42}}}'::json AS data $$) r; -- Expected: 42

-- Multiple paths to nested objects
SELECT public.json_extract('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}', ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]
-- </JSON_EXTRACT>

-- <JSON_EXTRACT_STRING>
-- Basic JSON Extraction
SELECT public.json_extract_string('{"key": "value"}', '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT public.json_extract_string('{"a": {"b": {"c": 42}}}', '$.a.b.c') AS result; -- Expected: 42
-- Multiple paths to nested objects
SELECT public.json_extract_string('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}', ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]

-- </JSON_EXTRACT_STRING>

-- Boolean and Numeric Values
SELECT public.json_extract_string('{"key": true}', '$.key') AS result; -- Expected: true
SELECT public.json_extract_string('{"key": 123}', '$.key') AS result; -- Expected: 123

-- </JSON_EXTRACT_STRING>

-- <JSON_VALUE>

-- Nested JSON Extraction
SELECT public.json_value('{"a": {"b": {"c": 42}}}', '$.a.b.c') AS result; -- Expected: 42

-- Non-existent Path
SELECT public.json_value('{"key": "value"}', '$.nonexistent') AS result; -- Expected: NULL

-- </JSON_VALUE>

-- <JSON_ARRAY_LENGTH>

-- Test with a JSON array at the root and using a JSON path
SELECT public.json_array_length('[1, 2, 3, 4, 5]', '$') AS array_length; -- Expected: 5

-- Test with a JSON object that doesn't contain an array at the path
SELECT public.json_array_length('{"not_an_array": {"key": "value"}}', '$.not_an_array') AS array_length; -- Expected: 0 (this  is expected DuckDB behaviour)

-- </JSON_ARRAY_LENGTH>

-- <JSON_CONTAINS>

-- Simple JSON array with numeric needle
SELECT public.json_contains('[1, 2, 3, 4]', '2') AS contains_numeric; -- Expected: TRUE

-- JSON object containing the needle as a value
SELECT public.json_contains('{"key1": "value1", "key2": 42}', '"value1"') AS contains_object_value; -- Expected: TRUE


-- </JSON_CONTAINS>

-- <JSON_Keys>
-- Test 1: Extract keys from a simple JSON object
SELECT public.json_keys('{"key1": "value1", "key2": "value2", "key3": "value3"}');

-- Test 2: Extract keys from an empty JSON object
SELECT public.json_keys('{}');

-- </JSON_Keys>


-- <JSON_STRUCTURE>
-- Test 1: Consistent structure (simple nested JSON object)
SELECT public.json_structure('{"name": "John", "age": 30, "address": {"city": "New York", "zip": "10001"}}')
AS structure;

-- Expected Output:
-- { "name": "string", "age": "number", "address": { "city": "string", "zip": "string" } }

-- Test 2: Inconsistent structure (array with incompatible types)
SELECT public.json_structure('{"data": [1, "string", {"key": "value"}]}')
AS structure;

-- Expected Output:
-- JSON (due to inconsistent types in the array)

-- </JSON_STRUCTURE>

-- <JSON_TYPE>
-- Test 1: Determine the type of the top-level JSON
SELECT public.json_type('{"name": "John", "age": 30, "isEmployed": true, "skills": ["SQL", "Python"]}')
AS top_level_type;

-- Expected Output:
-- OBJECT (because the top-level JSON is an object)

-- Test 2: Determine the types of elements at specific paths
SELECT public.json_type('{"name": "John", "age": 30, "isEmployed": true, "skills": ["SQL", "Python"]}', ARRAY['name', 'age', 'isEmployed', 'skills'])
AS element_types;

-- Expected Output:
-- LIST ['VARCHAR', 'BIGINT', 'BOOLEAN', 'ARRAY'] (corresponding to the types of the specified elements)

-- </JSON_TYPE>

-- <JSON_VALID>
-- Test 1: Valid JSON
SELECT public.json_valid('{"name": "John", "age": 30, "skills": ["SQL", "Python"]}') AS is_valid;

-- Expected Output:
-- true (since the JSON is well-formed)

-- Test 2: Invalid JSON
SELECT public.json_valid('{"name": "John", "age": 30, "skills": ["SQL", "Python"'::duckdb.json) AS is_valid;

-- Expected Output:
-- false

-- </JSON_VALID>

-- <JSON>
    -- SELECT json('{
    --     "name": "John",
    --     "age": 30,
    --     "skills": ["SQL", "Python"]
    -- }') AS minified_json;
-- </JSON>


CREATE TABLE example1 (k VARCHAR, v INTEGER);
INSERT INTO example1 VALUES ('duck', 42), ('goose', 7);
-- <JSON_GROUP_ARRAY>
SELECT public.json_group_array(v) FROM example1;
-- </JSON_GROUP_ARRAY>

-- <JSON_GROUP_OBJECT>
SELECT public.json_group_object(k, v) FROM example1;
SELECT public.json_group_object(123, 'abc');

-- </JSON_GROUP_OBJECT>

-- <JSON_GROUP_STRUCTURE>
CREATE TABLE example2 (j JSON);
INSERT INTO example2 VALUES
    ('{"family": "anatidae", "species": ["duck", "goose"], "coolness": 42.42}'),
    ('{"family": "canidae", "species": ["labrador", "bulldog"], "hair": true}');

SELECT json_group_structure(j) FROM example2;

-- </JSON_GROUP_STRUCTURE>

-- STRUCTURE no supported in postgres

-- CREATE TABLE example (j JSON);
-- INSERT INTO example VALUES
--     ('{"family": "anatidae", "species": ["duck", "goose"], "coolness": 42.42}'),
--     ('{"family": "canidae", "species": ["labrador", "bulldog"], "hair": true}');
-- -- <JSON_TRANSFORM>
-- SELECT json_transform(j, '{"family": "VARCHAR", "coolness": "DOUBLE"}') FROM example;

-- SELECT json_transform(j, '{"family": "TINYINT", "coolness": "DECIMAL(4, 2)"}') FROM example;
-- -- </JSON_TRANSFORM>

-- -- <JSON_TRANSFORM_STRICT>
-- SELECT json_transform_strict(j, '{"family": "TINYINT", "coolness": "DOUBLE"}') FROM example;
-- -- </JSON_TRANSFORM_STRICT>
