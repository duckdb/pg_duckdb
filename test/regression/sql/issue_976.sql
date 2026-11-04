-- Rows read from Postgres parallel workers by multiple DuckDB threads go
-- through GetNextMinimalWorkerTuple. It used to copy 10 bytes more than the
-- tuple queue message contains. ASan only reports that when the message was
-- reassembled into a buffer that ends a memory block; with 4 workers and
-- 63-byte tuples this happens on an ASan build of PG19 on macOS. Other
-- versions and platforms may need different sizes to hit it.
SET duckdb.force_execution = true;
SET duckdb.max_workers_per_postgres_scan = 4;
SET duckdb.threads_for_postgres_scan = 4;
CREATE TABLE issue_976 (w int, s text);
INSERT INTO issue_976 SELECT w, repeat('x', w) FROM generate_series(0, 299) w, generate_series(1, 3000) i;
SELECT count(*), sum(length(s)) FROM issue_976 WHERE w = 46;
DROP TABLE issue_976;
