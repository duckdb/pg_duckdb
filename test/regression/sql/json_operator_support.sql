-- Create Table
-- Create table for JSON testing
CREATE TABLE test_json (
    data JSONB
);

-- Insert test data
INSERT INTO test_json (data) VALUES
('{"a": 1, "b": {"c": 2, "d": [3, 4]}, "e": "hello"}'),
('{"f": 10, "g": {"h": 20, "i": 30}, "j": [40, 50, 60]}'),
('{"k": true, "l": null, "m": {"n": "world", "o": [7, 8, 9]}}'),
('[1, 2, 3]'),
('["a", "b", "c"]'),
('[{"key": "value"}, {"key": "another"}]');

-- Simple Select
select * from test_json;

-- Drop table
DROP TABLE test_json;
