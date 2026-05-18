SET duckdb.force_execution = true;
SET duckdb.threads_for_postgres_scan = 2;

CREATE TABLE issue_976_src (four int, ten int);
INSERT INTO issue_976_src
SELECT (i % 4), (i % 10)
FROM generate_series(1, 200000) AS i;

-- Parallel Postgres scan with multi-threaded DuckDB consumption (GetNextMinimalWorkerTuple).
-- Order-independent checks: parallel scans do not guarantee global ORDER BY semantics.
SELECT count(*)::bigint, count(DISTINCT (four, ten))::bigint FROM issue_976_src;
SELECT count(*)::bigint FROM (SELECT DISTINCT ten, four FROM issue_976_src) ss;

DROP TABLE issue_976_src;
