from .utils import Postgres, Cursor, run
import pytest


@pytest.mark.timeout(300)
def test_fuzzy_pg_crashing(pg: Postgres, cur: Cursor):
    cur.sql("CREATE DATABASE sqlsmith_test")
    with pg.cur(dbname="sqlsmith_test") as sqlsmith_cur:
        sqlsmith_cur.sql("CREATE EXTENSION pg_duckdb")
        # create test tables with dummy data to allow sqlsmith to do more operations
        sqlsmith_cur.sql("CREATE TABLE test_table (id INT, name TEXT)")
        sqlsmith_cur.sql("CREATE TABLE test_table2 (id INT, name VARCHAR)")

        sqlsmith_cur.sql(
            "INSERT INTO test_table (SELECT num, 'name_' || num FROM generate_series(1, 100) as num)"
        )
        sqlsmith_cur.sql(
            "INSERT INTO test_table2 (SELECT num, 'name_' || num FROM generate_series(10, 100) as num)"
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
