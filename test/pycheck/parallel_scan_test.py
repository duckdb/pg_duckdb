import os
import threading

import psycopg
import psycopg.sql
import pytest

from .utils import Cursor, Postgres


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
        ) == (5, 8)


@pytest.mark.timeout(240)
def test_parallel_worker_errors_in_many_scans(pg: Postgres):
    """
    Fail a query that scans many Postgres tables at once with errors raised in
    the Postgres parallel workers of those scans.

    Postgres reads the messages of all parallel workers of the backend in one
    go and stops at the first error. Postgres then aborts the transaction,
    but pg_duckdb catches the error and continues with the other scans. Their
    cleanup used to wait forever for messages of their own workers that were
    never read, so the query hung in ParallelFinish. Logging the failed
    cleanup could also raise the error again outside of any error handling.
    """
    pg.create_user("scan_reader", psycopg.sql.SQL("IN ROLE duckdb_group"))
    pg.sql(
        psycopg.sql.SQL("GRANT USAGE ON SCHEMA {} TO scan_reader").format(
            psycopg.sql.Identifier(pg.sql("SELECT current_schema()"))
        )
    )
    pg.sql("CREATE TABLE lockme (x int)")
    pg.sql("INSERT INTO lockme VALUES (1)")
    # The policy function takes a lock, so it fails in the workers when another session holds a conflicting lock
    pg.sql("""
        CREATE FUNCTION chk(id bigint) RETURNS boolean
        LANGUAGE plpgsql STABLE PARALLEL SAFE SECURITY DEFINER
        AS $$ BEGIN RETURN EXISTS (SELECT 1 FROM lockme); END $$
    """)
    num_tables = 40
    for i in range(num_tables):
        pg.sql(
            f"CREATE TABLE p{i} AS SELECT g::bigint AS id FROM generate_series(1, 2000) g"
        )
        pg.sql(f"ALTER TABLE p{i} ENABLE ROW LEVEL SECURITY")
        pg.sql(f"CREATE POLICY r ON p{i} FOR SELECT USING (chk(id))")
        pg.sql(f"GRANT SELECT ON p{i} TO scan_reader")
    pg.sql("ANALYZE")
    query = (
        "SELECT count(*) FROM ("
        + " UNION ALL ".join(f"SELECT id FROM p{i}" for i in range(num_tables))
        + ") x"
    )

    with pg.cur() as locker, pg.cur() as reader, pg.cur() as killer:
        reader.sql("SET ROLE scan_reader")
        reader.sql("SET duckdb.force_execution = true")
        reader.sql("SET lock_timeout = '100ms'")
        reader_pid = reader.sql("SELECT pg_backend_pid()")

        # A hanging query ignores cancellation, so terminate it to fail the test instead of blocking the test run
        hung = threading.Event()

        def terminate_reader():
            hung.set()
            killer.sql("SELECT pg_terminate_backend(%s)", (reader_pid,))

        locker.sql("BEGIN")
        locker.sql("LOCK lockme IN ACCESS EXCLUSIVE MODE")
        for _ in range(10):
            watchdog = threading.Timer(60, terminate_reader)
            watchdog.start()
            try:
                with pytest.raises(psycopg.Error) as exc_info:
                    reader.sql(query)
            finally:
                watchdog.cancel()
            assert not hung.is_set(), "the query hung"
            # The error must come through pg_duckdb's error handling
            assert "(PGDuckDB/" in str(exc_info.value)
            assert "lock timeout" in str(
                exc_info.value
            ) or "lost connection to parallel worker" in str(exc_info.value)
        locker.sql("COMMIT")

        # The backend still works fine afterwards
        assert reader.sql(query) == num_tables * 2000


def test_parallel_worker_error_after_limit(pg: Postgres):
    """
    Raise an error in the Postgres parallel worker of a scan after DuckDB
    already got the rows it needs for a LIMIT.

    The cleanup of the scan then catches that error, and the query continues.
    Postgres resets the interrupt holdoff counts when it raises an error,
    which used to leave the cancel interrupts that are held during DuckDB
    execution unheld and failed an assertion at the end of the scan. The rest
    of the cleanup was also skipped, which left the transaction in parallel
    mode, so later writes in the same transaction failed.
    """
    pg.create_user("limit_reader", psycopg.sql.SQL("IN ROLE duckdb_group"))
    pg.sql(
        psycopg.sql.SQL("GRANT USAGE ON SCHEMA {} TO limit_reader").format(
            psycopg.sql.Identifier(pg.sql("SELECT current_schema()"))
        )
    )
    # Fails in the parallel worker only, once it has sent more rows than DuckDB fetches at once
    pg.sql("""
        CREATE FUNCTION chk(id bigint) RETURNS boolean
        LANGUAGE plpgsql STABLE PARALLEL SAFE
        AS $$ BEGIN
            IF id > 2049 AND pg_backend_pid() <> current_setting('test.leader_pid')::int THEN
                RAISE EXCEPTION 'error in parallel worker';
            END IF;
            RETURN true;
        END $$
    """)
    pg.sql("CREATE TABLE t AS SELECT g::bigint AS id FROM generate_series(1, 60000) g")
    pg.sql("ALTER TABLE t ENABLE ROW LEVEL SECURITY")
    pg.sql("CREATE POLICY r ON t FOR SELECT USING (chk(id))")
    pg.sql("GRANT SELECT ON t TO limit_reader")
    pg.sql("ANALYZE t")
    pg.sql("CREATE TABLE w (x int)")
    pg.sql("GRANT INSERT, SELECT ON w TO limit_reader")

    with pg.log_path.open() as log:
        log.seek(0, os.SEEK_END)
        with pg.cur() as cur:
            notices = []
            cur.connection.add_notice_handler(
                lambda diag: notices.append(diag.message_primary)
            )
            cur.sql("SET ROLE limit_reader")
            cur.sql("SET duckdb.force_execution = true")
            cur.sql(
                "SELECT set_config('test.leader_pid', pg_backend_pid()::text, false)"
            )
            committed = 0
            for _ in range(20):
                cur.sql("BEGIN")
                try:
                    # Depending on timing the worker error is caught in the cleanup or fails the query
                    assert cur.sql("SELECT id FROM t LIMIT 1") == 1
                except psycopg.errors.InternalError:
                    cur.sql("ROLLBACK")
                    continue
                # The transaction must still be usable for writes
                cur.sql("INSERT INTO w VALUES (1)")
                cur.sql("COMMIT")
                committed += 1
            assert cur.sql("SELECT count(*) FROM w") == committed
            # Nothing of the scans is left behind
            assert notices == []
        # Make sure that the test hit the case of a worker error that is caught in the cleanup
        assert "Failed to clean up Postgres scan" in log.read()
