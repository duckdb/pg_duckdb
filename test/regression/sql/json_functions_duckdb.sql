-- <JSON_EXISTS>
-- Test 1: Path exists in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}', '$.a.b'); -- Expected: true

-- Test 2: Path does not exist in a simple JSON object
SELECT json_exists('{"a": {"b": 1}}', '$.a.c'); -- Expected: false

-- Test 3: Path does not exist in a deeply nested structure
SELECT json_exists('{"a": {"b": {"c": 2}}}', '$.a.b.d'); -- Expected: false

-- Test 4: Invalid JSON path
SELECT json_exists('{"a": {"b": 1}}', '$.a.b['); -- Expected: false (or error if path is invalid)

-- Test 5: JSON array path exists
SELECT json_exists('[{"a": 1}, {"b": 2}]', '$[0].a'); -- Expected: true

-- Test 6: JSON array path does not exist
SELECT json_exists('[{"a": 1}, {"b": 2}]', '$[1].c'); -- Expected: false

-- Test 7: Empty JSON
SELECT json_exists('{}', '$.a'); -- Expected: false

-- Test 8: Null JSON input
SELECT json_exists(NULL, '$.a'); -- Expected: false

-- Test 9: Null path input
SELECT json_exists('{"a": {"b": 1}}', NULL); -- Expected: false

-- Test 10: Both inputs null
SELECT json_exists(NULL, NULL); -- Expected: false

-- </JSON_EXISTS>

-- <JSON_EXTRACT>
-- Basic JSON Extraction
SELECT json_extract('{"key": "value"}', '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT json_extract('{"a": {"b": {"c": 42}}}', '$.a.b.c') AS result; -- Expected: 42

-- Array Indexing
SELECT json_extract('[1, 2, 3]', '$[1]') AS result; -- Expected: 2
SELECT json_extract('[1, [2, 3], 4]', '$[1][0]') AS result; -- Expected: 2

-- Non-existent Path
SELECT json_extract('{"key": "value"}', '$.nonexistent') AS result; -- Expected: NULL

-- Empty and Null JSON
SELECT json_extract('{}', '$.key') AS result; -- Expected: NULL
SELECT json_extract('null', '$.key') AS result; -- Expected: NULL

-- Invalid JSON
SELECT json_extract('{key: "value"}', '$.key') AS result; -- Expected: Error

-- Escaped Characters
SELECT json_extract('{"escaped_key": "\"value\""}', '$.escaped_key') AS result; -- Expected: "\"value\""

-- Path with Special Characters
SELECT json_extract('{"a.b": {"c.d": 42}}', '$."a.b"."c.d"') AS result; -- Expected: 42

-- Mixing Object and Array
SELECT json_extract('{"key": [1, 2, {"nested": "value"}]}', '$.key[2].nested') AS result; -- Expected: "value"

-- Complex Nested Structures
SELECT json_extract('{"a": {"b": {"c": {"d": {"e": "deep"}}}}}', '$.a.b.c.d.e') AS result; -- Expected: "deep"

-- Using Null or Empty Path
SELECT json_extract('{"key": "value"}', NULL) AS result; -- Expected: Error
SELECT json_extract('{"key": "value"}', '') AS result; -- Expected: Error

-- Path as List
-- Extract multiple paths into a list of values
SELECT json_extract('{"a": {"b": 1}, "x": 42}', ARRAY['$.a.b', '$.x']) AS result; -- Expected: [1, 42]

-- Mixed valid and invalid paths in the list
SELECT json_extract('{"a": {"b": 1}, "x": 42}', ARRAY['$.a.b', '$.nonexistent']) AS result; -- Expected: [1, NULL]

-- Multiple paths to nested objects
SELECT json_extract('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}', ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]

-- Path list with array indexing
SELECT json_extract('[{"key": 1}, {"key": 2}]', ARRAY['$[0].key', '$[1].key']) AS result; -- Expected: [1, 2]

-- Boolean and Numeric Values
SELECT json_extract('{"key": true}', '$.key') AS result; -- Expected: true
SELECT json_extract('{"key": 123}', '$.key') AS result; -- Expected: 123

-- </JSON_EXTRACT>

-- <JSON_EXTRACT_STRING>
-- Basic JSON Extraction
SELECT json_extract_string('{"key": "value"}', '$.key') AS result; -- Expected: "value"

-- Nested JSON Extraction
SELECT json_extract_string('{"a": {"b": {"c": 42}}}', '$.a.b.c') AS result; -- Expected: 42

-- Array Indexing
SELECT json_extract_string('[1, 2, 3]', '$[1]') AS result; -- Expected: 2
SELECT json_extract_string('[1, [2, 3], 4]', '$[1][0]') AS result; -- Expected: 2

-- Non-existent Path
SELECT json_extract_string('{"key": "value"}', '$.nonexistent') AS result; -- Expected: NULL

-- Empty and Null JSON
SELECT json_extract_string('{}', '$.key') AS result; -- Expected: NULL
SELECT json_extract_string('null', '$.key') AS result; -- Expected: NULL

-- Invalid JSON
SELECT json_extract_string('{key: "value"}', '$.key') AS result; -- Expected: Error

-- Escaped Characters
SELECT json_extract_string('{"escaped_key": "\"value\""}', '$.escaped_key') AS result; -- Expected: "\"value\""

-- Path with Special Characters
SELECT json_extract_string('{"a.b": {"c.d": 42}}', '$."a.b"."c.d"') AS result; -- Expected: 42

-- Mixing Object and Array
SELECT json_extract_string('{"key": [1, 2, {"nested": "value"}]}', '$.key[2].nested') AS result; -- Expected: "value"

-- Complex Nested Structures
SELECT json_extract_string('{"a": {"b": {"c": {"d": {"e": "deep"}}}}}', '$.a.b.c.d.e') AS result; -- Expected: "deep"

-- Using Null or Empty Path
SELECT json_extract_string('{"key": "value"}', NULL) AS result; -- Expected: Error
SELECT json_extract_string('{"key": "value"}', '') AS result; -- Expected: Error

-- Path as List
-- Extract multiple paths into a list of values
SELECT json_extract_string('{"a": {"b": 1}, "x": 42}', ARRAY['$.a.b', '$.x']) AS result; -- Expected: [1, 42]

-- Mixed valid and invalid paths in the list
SELECT json_extract_string('{"a": {"b": 1}, "x": 42}', ARRAY['$.a.b', '$.nonexistent']) AS result; -- Expected: [1, NULL]

-- Multiple paths to nested objects
SELECT json_extract_string('{"a": {"b": {"c": 42}}, "x": {"y": "value"}}', ARRAY['$.a.b.c', '$.x.y']) AS result; -- Expected: [42, "value"]

-- Path list with array indexing
SELECT json_extract_string('[{"key": 1}, {"key": 2}]', ARRAY['$[0].key', '$[1].key']) AS result; -- Expected: [1, 2]

-- Boolean and Numeric Values
SELECT json_extract_string('{"key": true}', '$.key') AS result; -- Expected: true
SELECT json_extract_string('{"key": 123}', '$.key') AS result; -- Expected: 123

-- </JSON_EXTRACT_STRING>
