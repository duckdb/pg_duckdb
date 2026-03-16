-- Test duckdb.allowed_directories GUC with comma-separated values
-- Recycle DuckDB first so GucCheckDuckDBNotInitdHook allows SET
CALL duckdb.recycle_ddb();
SET duckdb.allowed_directories = 's3://test-bucket/data/, /tmp/duckdb-test/';
SET duckdb.enable_external_access = false;

-- Should be blocked (not in allowed dirs)
SELECT * FROM duckdb.raw_query($$ SELECT * FROM read_csv('/etc/passwd') $$);
SELECT * FROM duckdb.raw_query($$ SELECT * FROM read_csv('https://example.com/test.csv') $$);
SELECT * FROM duckdb.raw_query($$ SELECT * FROM read_csv('s3://other-bucket/secret.csv') $$);

-- Cleanup: recycle to clear restrictions for subsequent tests
CALL duckdb.recycle_ddb();
