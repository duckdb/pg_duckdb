-- JSON
set duckdb.force_execution to true;

-- -> int operator
select '[{"a":"foo"},{"b":"bar"},{"c":"baz"}]'::json->2;

-- -> text operator
select '{"a": {"b":"foo"}}'::json->'a'

-- #>> text[] operator
select '{"a":[1,2,3],"b":[4,5,6]}'::json#>>'{a,2}';

-- #> text[] operator
select '{"a": {"b":{"c": "foo"}}}'::json#>'{a,b}';

-- ->> int operator
select '[1,2,3]'::json->>2;

-- ->> text operator
select '{"a":1,"b":2}'::json->>'b';

-- 

-- Create Table
-- Create table for JSON testing
CREATE TABLE test_json (
    id SERIAL PRIMARY KEY,
    data JSONB
);

-- Insert test data
INSERT INTO test_json (data) VALUES
('{"a": 1, "b": {"c": 2, "d": [3, 4]}, "e": "hello"}'),
('{"f": 10, "g": {"h": 20, "i": 30}, "j": [40, 50, 60]}'),
('{"k": true, "l": null, "m": {"n": "world", "o": [7, 8, 9]}}');

-- Test Case 1: Access JSON Object Field (->)
SELECT id, data->'a' AS a_value
FROM test_json;

-- Test Case 2: Access JSON Object Field as Text (->>)
SELECT id, data->>'e' AS e_value
FROM test_json;

-- Test Case 3: Access Nested JSON Object Field (#>)
SELECT id, data#>'{b, c}' AS b_c_value
FROM test_json;

-- Test Case 4: Access Nested JSON Object Field as Text (#>>)
SELECT id, data#>>'{m, n}' AS m_n_value
FROM test_json;

-- Test Case 5: Check for Key Existence (?)
SELECT id, data ? 'k' AS has_k
FROM test_json;

-- Test Case 6: Check for Any Key in a List (?|)
SELECT id, data ?| ARRAY['a', 'f', 'x'] AS has_any_key
FROM test_json;

-- Test Case 7: Check for All Keys in a List (?&)
SELECT id, data ?& ARRAY['a', 'b'] AS has_all_keys
FROM test_json;

-- Test Case 8: Concatenate JSON Objects (||)
SELECT id, data || '{"new_key": "new_value"}' AS updated_data
FROM test_json;

-- Test Case 9: Delete Key/Value Pair (-)
SELECT id, data - 'a' AS without_a
FROM test_json;

-- Test Case 10: Delete Multiple Key/Value Pairs (- ARRAY[])
SELECT id, data - ARRAY['a', 'b'] AS without_a_b
FROM test_json;

-- Test Case 11: Filter by JSON Value (@>)
SELECT id, data @> '{"a": 1}' AS contains_a_1
FROM test_json;

-- Test Case 12: Check if Contained Within (<@)
SELECT id, data <@ '{"a": 1, "b": {"c": 2, "d": [3, 4]}, "e": "hello"}' AS is_subset
FROM test_json;

-- Test Case 13: Extract Array Element by Index (#> for Arrays)
SELECT id, data#>'{b, d, 1}' AS second_element_in_d
FROM test_json;

-- Test Case 14: Filter Using JSON Path Queries (@?)
SELECT id, data @? '$.b.d[*] ? (@ > 3)' AS has_d_value_greater_than_3
FROM test_json;

-- Test Case 15: Extract Value Using JSON Path Queries (@@)
SELECT id, data @@ '$.k == true' AS k_is_true
FROM test_json;




