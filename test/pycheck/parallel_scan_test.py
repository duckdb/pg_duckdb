from .utils import Cursor


def test_repeated_parallel_postgres_scan(cur: Cursor):
    """
    Repeatedly run a DuckDB query that scans a Postgres table using Postgres
    parallel workers.

    On macOS the Postgres code that sets up the parallel scan saves and
    restores the signal mask with sigprocmask(), which changes the mask of all
    threads of the process. When that code ran on a DuckDB thread, the backend
    main thread ended up with all signals blocked, so the notification that a
    parallel worker finished was never handled and the query hung forever.
    Which thread runs the scan setup varies between runs, so the query is
    repeated to make hitting the problematic case very likely.
    """
    cur.sql("CREATE TABLE t (a int, b text)")
    cur.sql(
        "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e'), (2, 'f'), (3, 'g'), (4, 'h')"
    )
    cur.sql("SET duckdb.force_execution = true")
    for _ in range(50):
        assert cur.sql(
            "SELECT approx_count_distinct(a), approx_count_distinct(b) FROM t"
        ) == (5, 9)
