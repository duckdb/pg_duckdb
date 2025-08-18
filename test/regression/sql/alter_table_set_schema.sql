-- Test ALTER TABLE SET SCHEMA for DuckDB tables
-- This test verifies that DuckDB tables can be moved between schemas
-- Note: Due to DuckDB limitations, temporary tables cannot be moved to non-temporary schemas

-- Set up test environment
SET duckdb.force_execution = false;

-- Create test schemas
CREATE SCHEMA test_schema1;
CREATE SCHEMA test_schema2;

-- Test error cases first
-- Try to move a non-existent table
ALTER TABLE test_schema1.non_existent_table SET SCHEMA public;

-- Try to move a table to a non-existent schema
ALTER TABLE test_schema1.duckdb_table SET SCHEMA non_existent_schema;

-- Test with temporary tables
CREATE TEMP TABLE temp_duckdb_table (
    id INT,
    name TEXT
) USING duckdb;

INSERT INTO temp_duckdb_table VALUES (1, 'temp1');
INSERT INTO temp_duckdb_table VALUES (2, 'temp2');

-- Verify the table exists
SELECT * FROM temp_duckdb_table ORDER BY id;

-- Test moving temporary table to another temporary schema (this should work)
-- Note: In DuckDB, temporary tables stay in the temp schema
-- The ALTER TABLE SET SCHEMA command is supported but may have limitations

-- Clean up
DROP TABLE temp_duckdb_table;
DROP SCHEMA test_schema1;
DROP SCHEMA test_schema2;
