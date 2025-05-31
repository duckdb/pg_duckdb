from .utils import Postgres, Cursor, run
import pytest


@pytest.mark.timeout(300)
def test_fuzzy_pg_crashing(pg: Postgres, cur: Cursor):
    cur.sql("CREATE DATABASE sqlsmith_test")
    with pg.cur(dbname="sqlsmith_test") as sqlsmith_cur:
        sqlsmith_cur.sql("CREATE EXTENSION pg_duckdb")
        # create a test table with arbitary data to allow sqlsmith to do more operations
        sqlsmith_cur.sql("CREATE TABLE test_table (id INT, name TEXT)")
        sqlsmith_cur.sql(
            "INSERT INTO test_table (id, name) VALUES (1, 'Alice'), (2, 'Bob')"
        )

    # any crash caused by sqlsmith will be detected from logs on the teardown of pg fixture
    run(
        [
            "/usr/bin/sqlsmith",
            f"--target=host={pg.host} port={pg.port} user={pg.default_user} dbname=sqlsmith_test",
            "--max-queries=1500",
        ],
        check=True,
    )

    cur.sql("DROP DATABASE sqlsmith_test")
