-- Test duckdb.allowed_directories GUC with comma-separated values
SET duckdb.allowed_directories = '../../data/, s3://test-bucket/data/';
SET duckdb.enable_external_access = false;

-- Should be allowed (the directory is in allowed_directories)
SELECT count(*) FROM read_csv('../../data/iris.csv');

-- Should be blocked (not in allowed dirs)
SELECT * FROM read_csv('/etc/passwd');
SELECT * FROM read_csv('https://example.com/test.csv');
SELECT * FROM read_csv('s3://other-bucket/secret.csv');
