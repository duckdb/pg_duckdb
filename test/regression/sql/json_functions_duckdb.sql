-- <JSON_EXISTS>
-- Test 1: Path exists in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}'::json, '$.a.b'); -- Expected: true

-- Test 2: Path does not exist in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}'::json, '$.a.c'); -- Expected: false

-- Test 1: Path exists in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}'::jsonb, '$.a.b'); -- Expected: true

-- Test 2: Path does not exist in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}'::jsonb, '$.a.c'); -- Expected: false

-- </JSON_EXISTS>

-- <JSON_EXTRACT>
-- Basic JSON Extraction
SELECT json_extract('{"key": "value"}'::json, '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT json_extract('{"a": {"b": {"c": 42}}}'::json, '$.a.b.c') AS result; -- Expected: 42

-- Multiple paths to nested objects
SELECT json_extract('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}'::json, ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]
-- </JSON_EXTRACT>

-- <JSON_EXTRACT_STRING>
-- Basic JSON Extraction
SELECT json_extract_string('{"key": "value"}'::json, '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT json_extract_string('{"a": {"b": {"c": 42}}}'::json, '$.a.b.c') AS result; -- Expected: 42
-- Multiple paths to nested objects
SELECT json_extract_string('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}'::json, ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]

-- </JSON_EXTRACT_STRING>

-- Boolean and Numeric Values
SELECT json_extract_string('{"key": true}'::json, '$.key') AS result; -- Expected: true
SELECT json_extract_string('{"key": 123}'::json, '$.key') AS result; -- Expected: 123

-- </JSON_EXTRACT_STRING>

-- <JSON_VALUE>

-- Basic Scalar Value Extraction
SELECT json_value('{"key": "value"}'::json, '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT json_value('{"a": {"b": {"c": 42}}}'::json, '$.a.b.c') AS result; -- Expected: 42

-- Non-existent Path
SELECT json_value('{"key": "value"}'::json, '$.nonexistent') AS result; -- Expected: NULL

-- </JSON_VALUE>

-- <JSON_ARRAY_LENGTH>

-- Test with a JSON array at the root and using a JSON path
SELECT json_array_length('[1, 2, 3, 4, 5]'::json, '$') AS array_length; -- Expected: 5

-- Test with a JSON object that doesn't contain an array at the path
SELECT json_array_length('{"not_an_array": {"key": "value"}}'::json, '$.not_an_array') AS array_length; -- Expected: Error or NULL

-- </JSON_ARRAY_LENGTH>

-- <JSON_CONTAINS>

-- Simple JSON array with numeric needle
SELECT json_contains('[1, 2, 3, 4]'::json, '2') AS contains_numeric; -- Expected: TRUE

-- JSON object containing the needle as a value
SELECT json_contains('{"key1": "value1", "key2": 42}'::json, '"value1"') AS contains_object_value; -- Expected: TRUE


-- </JSON_CONTAINS>

-- <JSON_Keys>
-- Test 1: Extract keys from a simple JSON object
SELECT json_keys('{"key1": "value1", "key2": "value2", "key3": "value3"}'::JSON);

-- Test 2: Extract keys from an empty JSON object
SELECT json_keys('{}'::JSON);

-- </JSON_Keys>


-- <JSON_STRUCTURE>
-- Test 1: Consistent structure (simple nested JSON object)
SELECT json_structure('{"name": "John", "age": 30, "address": {"city": "New York", "zip": "10001"}}'::JSON)
AS structure;

-- Expected Output:
-- { "name": "string", "age": "number", "address": { "city": "string", "zip": "string" } }

-- Test 2: Inconsistent structure (array with incompatible types)
SELECT json_structure('{"data": [1, "string", {"key": "value"}]}'::JSON)
AS structure;

-- Expected Output:
-- JSON (due to inconsistent types in the array)

-- </JSON_STRUCTURE>

-- <JSON_TYPE>
-- Test 1: Determine the type of the top-level JSON
SELECT json_type('{"name": "John", "age": 30, "isEmployed": true, "skills": ["SQL", "Python"]}'::JSON)
AS top_level_type;

-- Expected Output:
-- OBJECT (because the top-level JSON is an object)

-- Test 2: Determine the types of elements at specific paths
SELECT json_type('{"name": "John", "age": 30, "isEmployed": true, "skills": ["SQL", "Python"]}'::JSON, ARRAY['name', 'age', 'isEmployed', 'skills'])
AS element_types;

-- Expected Output:
-- LIST ['VARCHAR', 'BIGINT', 'BOOLEAN', 'ARRAY'] (corresponding to the types of the specified elements)

-- </JSON_TYPE>

-- <JSON_VALID>
-- Test 1: Valid JSON
SELECT json_valid('{"name": "John", "age": 30, "skills": ["SQL", "Python"]}'::JSON) AS is_valid;

-- Expected Output:
-- true (since the JSON is well-formed)

-- Test 2: Invalid JSON
SELECT json_valid('{"name": "John", "age": 30, "skills": ["SQL", "Python"'::JSON) AS is_valid;

-- Expected Output:
-- false

-- </JSON_VALID>

-- <JSON>
    SELECT json('{
        "name": "John",
        "age": 30,
        "skills": ["SQL", "Python"]
    }'::JSON) AS minified_json;
-- </JSON>


CREATE TABLE example1 (k VARCHAR, v INTEGER);
INSERT INTO example1 VALUES ('duck', 42), ('goose', 7);
-- <JSON_GROUP_ARRAY>
SELECT json_group_array(v) FROM example1;
-- </JSON_GROUP_ARRAY>

-- <JSON_GROUP_OBJECT>
SELECT json_group_object(k, v) FROM example1;

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
