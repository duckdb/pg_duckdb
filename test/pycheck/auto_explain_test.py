import psycopg.errors
import pytest

from .utils import Cursor, Postgres


@pytest.mark.parametrize(
    "query, scan_query",
    [
        (
            "SELECT grp, count(DISTINCT buyer) FROM t GROUP BY grp",
            r"SELECT grp, buyer FROM \S+\.t",
        ),
        # A scan that only counts tuples
        ("SELECT count(*) FROM t", r"SELECT COUNT\(\*\) FROM \S+\.t"),
    ],
    ids=["group_by", "count"],
)
def test_auto_explain_logs_postgres_scan(pg: Postgres, cur: Cursor, query, scan_query):
    """
    auto_explain logs the Postgres scans of EXPLAIN ANALYZE with their query text (#1150)

    For EXPLAIN ANALYZE, DuckDB runs the query and its Postgres scans outside
    of the executor, so auto_explain logs the scans as top-level queries when
    they end. The query text of a scan used to be freed before that, which is
    only reported on ASan builds: without ASan the freed memory usually still
    holds the text.
    """
    try:
        cur.sql("LOAD 'auto_explain'")
    except psycopg.errors.UndefinedFile:
        pytest.skip("auto_explain is not installed")
    cur.sql("CREATE TABLE t (grp int, buyer int)")
    cur.sql("INSERT INTO t VALUES (1, 1), (1, 1), (1, 2), (2, 3)")
    cur.sql("SET auto_explain.log_min_duration = 0")
    cur.sql("SET auto_explain.log_format = json")
    cur.sql("SET duckdb.force_execution = true")
    # The scan is logged with the right query text
    with pg.log_contains(f'"Query Text": "{scan_query}"'):
        cur.sql(f"EXPLAIN (ANALYZE, FORMAT JSON) {query}")
