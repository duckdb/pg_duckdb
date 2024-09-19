from .utils import Cursor

import datetime


def test_prepared(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int)")

    # Try prepared query without parameters
    q1 = "SELECT count(*) FROM test_table"
    assert cur.sql(q1, prepare=True) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0

    cur.sql("INSERT INTO test_table VALUES (1), (2), (3)")
    assert cur.sql(q1) == 3

    # The following tests a prepared query that has parameters.
    # There are two ways in which prepared queries that have parameters can be
    # executed:
    # 1. With a custom plan, where the query is prepared with the exact values
    # 2. With a generic plan, where the query is planned without the values and
    #    the values get only substituted at execution time
    #
    # The below tests both of these cases, by setting the plan_cache_mode.
    q2 = "SELECT count(*) FROM test_table where id = %s"
    cur.sql("SET plan_cache_mode = 'force_custom_plan'")
    assert cur.sql(q2, (1,), prepare=True) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (3,)) == 1
    assert cur.sql(q2, (4,)) == 0

    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    assert cur.sql(q2, (1,)) == 1  # creates generic plan
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (3,)) == 1
    assert cur.sql(q2, (4,)) == 0


def test_extended(cur: Cursor):
    cur.sql("""
        CREATE TABLE t(
            bool BOOLEAN,
            i2 SMALLINT,
            i4 INT,
            i8 BIGINT,
            fl4 REAL,
            fl8 DOUBLE PRECISION,
            t1 TEXT,
            t2 VARCHAR,
            t3 BPCHAR,
            d DATE,
            ts TIMESTAMP);
        """)

    row = (
        True,
        2,
        4,
        8,
        4.0,
        8.0,
        "t1",
        "t2",
        "t3",
        datetime.date(2024, 5, 4),
        datetime.datetime(2020, 1, 1, 1, 2, 3),
    )
    cur.sql("INSERT INTO t VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)

    assert (True,) * len(row) == cur.sql(
        """
        SELECT
            bool = %s,
            i2 = %s,
            i4 = %s,
            i8 = %s,
            fl4 = %s,
            fl8 = %s,
            t1 = %s,
            t2 = %s,
            t3 = %s,
            d = %s,
            ts = %s
        FROM t;
        """,
        row,
    )
