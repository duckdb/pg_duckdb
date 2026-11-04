import random
import shutil

import pytest

from .utils import Cursor, Postgres, run

SQLSMITH = shutil.which("sqlsmith")


@pytest.mark.skipif(SQLSMITH is None, reason="sqlsmith is not installed")
@pytest.mark.timeout(300)
def test_fuzzy_pg_crashing(pg: Postgres, cur: Cursor):
    cur.sql("DROP DATABASE IF EXISTS sqlsmith_test")
    cur.sql("CREATE DATABASE sqlsmith_test")
    try:
        # avoid a single slow generated query hanging the whole test
        cur.sql("ALTER DATABASE sqlsmith_test SET statement_timeout = '10s'")
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

        # log the seed so that a failing run can be reproduced
        seed = random.randrange(2**31)
        print(f"sqlsmith seed: {seed}")

        # any crash caused by sqlsmith will be detected from logs on the teardown of pg fixture
        run(
            [
                SQLSMITH,
                f"--target=host={pg.host} port={pg.port} user={pg.default_user} dbname=sqlsmith_test",
                f"--seed={seed}",
                "--max-queries=1500",
            ],
            check=True,
        )
    finally:
        cur.sql("DROP DATABASE IF EXISTS sqlsmith_test WITH (FORCE)")
