-- Test ALTER TABLE SET SCHEMA for DuckDB tables with MotherDuck
-- This test covers basic ALTER TABLE SET SCHEMA functionality
-- Note: Due to DuckDB limitations, some cross-database operations may not work

-- Set up test environment
SET duckdb.force_execution = false;

-- Create test schemas in the default database
CREATE SCHEMA default_schema1;
CREATE SCHEMA default_schema2;

-- Test error cases for cross-database operations
-- Try to move a table to a non-existent database
ALTER TABLE ddb$test_db$schema1.default_table SET SCHEMA ddb$non_existent_db$schema1;

-- Try to move a table to a non-existent schema in an existing database
ALTER TABLE ddb$test_db$schema1.default_table SET SCHEMA ddb$test_db$non_existent_schema;

-- Clean up
DROP SCHEMA default_schema1;
DROP SCHEMA default_schema2;
