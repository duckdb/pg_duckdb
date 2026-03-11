-- Test duckdb.allowed_directories GUC with comma-separated values.
-- The list intentionally contains empty entries (a doubled comma and a
-- whitespace-only entry) to verify they are skipped, rather than silently
-- canonicalized to the Postgres data directory (the backend's CWD).
SET duckdb.allowed_directories = '../../data/,, ,s3://test-bucket/data/';
SET duckdb.enable_external_access = false;

-- Should be allowed (the directory is in allowed_directories)
SELECT count(*) FROM read_csv('../../data/iris.csv');

-- Should be blocked: the empty entries must not have allowlisted the data
-- directory, so a file that lives there (resolved relative to the backend's
-- CWD) stays inaccessible.
SELECT * FROM read_csv('PG_VERSION');

-- Should be blocked (not in allowed dirs)
SELECT * FROM read_csv('/etc/passwd');
SELECT * FROM read_csv('https://example.com/test.csv');
SELECT * FROM read_csv('s3://other-bucket/secret.csv');
