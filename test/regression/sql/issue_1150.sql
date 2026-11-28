-- The Postgres scans of DuckDB used to pass a temporary string as query text
-- to CreateQueryDesc. auto_explain reads that text in ExecutorEnd when the
-- scan ends, after it was freed. For EXPLAIN ANALYZE, DuckDB runs its scans
-- outside of the executor, so auto_explain logs them as top-level queries.
-- This is only reported on ASan builds, without ASan the freed memory usually
-- still holds the text.
CREATE TABLE t (grp int, buyer int);
INSERT INTO t VALUES (1, 1), (1, 1), (1, 2), (2, 3);
LOAD 'auto_explain';
SET auto_explain.log_min_duration = 0;
SET auto_explain.log_format = json;
-- The DO blocks hide the timings in the EXPLAIN ANALYZE output
DO $$
BEGIN
	EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) SELECT grp, count(DISTINCT buyer) FROM t GROUP BY grp';
END
$$;
-- A scan that only counts tuples
DO $$
BEGIN
	EXECUTE 'EXPLAIN (ANALYZE, FORMAT JSON) SELECT count(*) FROM t';
END
$$;
RESET auto_explain.log_min_duration;
RESET auto_explain.log_format;
SELECT grp, count(DISTINCT buyer) FROM t GROUP BY grp ORDER BY grp;
DROP TABLE t;
