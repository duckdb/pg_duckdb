import datetime
from decimal import Decimal
import uuid

import psycopg.errors
import psycopg.types.json
import pytest

from .utils import Connection, Cursor


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


def test_prepared_select_list_parameters(cur: Cursor):
    cur.sql("CREATE TEMP TABLE t_select_param (id int, name text) USING duckdb")
    cur.sql("INSERT INTO t_select_param VALUES (1, 'alice'), (2, 'bob'), (42, 'charlie')")

    expected_rows = [
        ("my_label", 1, "alice"),
        ("my_label", 2, "bob"),
        ("my_label", 42, "charlie"),
    ]

    for mode in ("force_custom_plan", "force_generic_plan"):
        cur.sql(f"SET plan_cache_mode = '{mode}'")

        q_select = "SELECT %s AS label, id, name FROM t_select_param ORDER BY id"
        assert cur.sql(q_select, ("my_label",), prepare=True) == expected_rows

        q_select_where = "SELECT %s AS label, id, name FROM t_select_param WHERE id = %s"
        assert cur.sql(q_select_where, ("my_label", 42), prepare=True) == [("my_label", 42, "charlie")]


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
            ivl INTERVAL,
            time TIME,
            timetz TIMETZ,
            d DATE,
            ts TIMESTAMP,
            tstz TIMESTAMP WITH TIME ZONE,
            json_obj JSON,
            u UUID);
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
        datetime.timedelta(days=5, hours=3, minutes=30),
        datetime.time(1, 2, 3),
        datetime.time(1, 2, 3, tzinfo=datetime.timezone(datetime.timedelta(hours=-5))),
        datetime.date(2024, 5, 4),
        datetime.datetime(2020, 1, 1, 1, 2, 3),
        datetime.datetime(2020, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc),
        psycopg.types.json.Json({"a": 1}),
        uuid.UUID("12345678-1234-5678-1234-567812345678"),
    )
    cur.sql(
        "INSERT INTO t VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        row,
    )

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
            ivl = %s,
            time = %s,
            timetz = %s,
            d = %s,
            ts = %s,
            tstz = %s,
            json_obj::text = %s::text,
            u = %s
        FROM t;
        """,
        row,
    )


def test_prepared_numeric_parameter(cur: Cursor):
    cur.sql("CREATE TABLE t_numeric(val NUMERIC)")
    cur.sql("INSERT INTO t_numeric VALUES (%s)", (Decimal("123.456"),))
    cur.sql("INSERT INTO t_numeric VALUES (%s)", (Decimal("999999.99"),))
    cur.sql("INSERT INTO t_numeric VALUES (%s)", (Decimal("-42.0"),))

    cur.sql("SET plan_cache_mode = 'force_custom_plan'")
    q = "SELECT count(*) FROM t_numeric WHERE val = %s"
    assert cur.sql(q, (Decimal("123.456"),), prepare=True) == 1
    assert cur.sql(q, (Decimal("999999.99"),)) == 1
    assert cur.sql(q, (Decimal("-42.0"),)) == 1
    assert cur.sql(q, (Decimal("0"),)) == 0

    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    assert cur.sql(q, (Decimal("123.456"),)) == 1  # creates generic plan
    assert cur.sql(q, (Decimal("999999.99"),)) == 1
    assert cur.sql(q, (Decimal("-42.0"),)) == 1
    assert cur.sql(q, (Decimal("0"),)) == 0


def test_prepared_array_parameters(cur: Cursor):
    cur.sql("CREATE TABLE t_int_arr(vals INT[])")
    cur.sql("INSERT INTO t_int_arr VALUES (%s)", ([1, 2, 3],))
    cur.sql("INSERT INTO t_int_arr VALUES (%s)", ([4, 5],))

    cur.sql("SET plan_cache_mode = 'force_custom_plan'")
    q_int = "SELECT count(*) FROM t_int_arr WHERE vals = %s"
    assert cur.sql(q_int, ([1, 2, 3],), prepare=True) == 1
    assert cur.sql(q_int, ([4, 5],)) == 1
    assert cur.sql(q_int, ([1, 2],)) == 0

    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    assert cur.sql(q_int, ([1, 2, 3],)) == 1  # creates generic plan
    assert cur.sql(q_int, ([4, 5],)) == 1
    assert cur.sql(q_int, ([1, 2],)) == 0

    cur.sql("CREATE TABLE t_text_arr(vals TEXT[])")
    cur.sql("INSERT INTO t_text_arr VALUES (%s)", (["hello", "world"],))
    cur.sql("INSERT INTO t_text_arr VALUES (%s)", (["foo"],))

    cur.sql("SET plan_cache_mode = 'force_custom_plan'")
    q_text = "SELECT count(*) FROM t_text_arr WHERE vals = %s"
    assert cur.sql(q_text, (["hello", "world"],), prepare=True) == 1
    assert cur.sql(q_text, (["foo"],)) == 1
    assert cur.sql(q_text, (["bar"],)) == 0

    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    assert cur.sql(q_text, (["hello", "world"],)) == 1  # creates generic plan
    assert cur.sql(q_text, (["foo"],)) == 1

    # Test NUMERIC[] arrays (special case with per-element precision)
    cur.sql("CREATE TABLE t_numeric_arr(vals NUMERIC[])")
    cur.sql(
        "INSERT INTO t_numeric_arr VALUES (%s)", ([Decimal("1.1"), Decimal("2.2")],)
    )

    cur.sql("SET plan_cache_mode = 'force_custom_plan'")
    q_num = "SELECT count(*) FROM t_numeric_arr WHERE vals = %s"
    assert cur.sql(q_num, ([Decimal("1.1"), Decimal("2.2")],), prepare=True) == 1

    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    assert cur.sql(q_num, ([Decimal("1.1"), Decimal("2.2")],)) == 1  # creates generic plan


@pytest.mark.parametrize("type_sql,value", [
    ("oid", 42),
    ("name", "myname"),
])
def test_prepared_unsupported_parameter_type(cur: Cursor, type_sql, value):
    cur.sql(f"CREATE TABLE t(x {type_sql}) USING duckdb")
    cur.sql("INSERT INTO t VALUES (%s)", (value,))
    cur.sql("SET plan_cache_mode = 'force_generic_plan'")
    q = "SELECT count(*) FROM t WHERE x = %s"
    with pytest.raises(
        psycopg.errors.InternalError,
        match="Could not convert Postgres parameter of type",
    ):
        cur.sql(q, (value,), prepare=True)


def test_prepared_writes(cur: Cursor):
    cur.sql("CREATE TEMP TABLE test_table (id int)")
    cur.sql("INSERT INTO test_table VALUES (%s), (%s), (%s)", (1, 2, 3))
    assert cur.sql("SELECT * FROM test_table ORDER BY id") == [1, 2, 3]


def test_prepared_pipeline(conn: Connection):
    with conn.pipeline() as p, conn.cursor() as cur:
        cur = Cursor(cur)
        cur.execute("CREATE TEMP TABLE heapt (id int)")
        p.sync()
        cur.execute("CREATE TEMP TABLE duckt (id int) using duckdb")
        p.sync()

        # These all auto-commit, so they complete their pipeline immediately
        # and should succeed
        cur.execute("INSERT INTO duckt VALUES (%s), (%s), (%s)", (1, 2, 3))
        cur.execute("DELETE FROM duckt WHERE id = %s", (2,))
        cur.execute("INSERT INTO duckt VALUES (%s)", (4,))
        assert cur.sql("SELECT * FROM duckt ORDER BY id") == [1, 3, 4]
        p.sync()

        # But if we first insert into the heap table, then try to insert into
        # the duckdb table that should fail because the insert into the heap
        # table opens an implicit transaction.
        with pytest.raises(
            psycopg.errors.InternalError,
            match="Writing to DuckDB and Postgres tables in the same transaction block is not supported",
        ):
            cur.execute("INSERT INTO heapt VALUES (%s), (%s), (%s)", (1, 2, 3))
            cur.execute("INSERT INTO duckt VALUES (%s)", (5,))
            p.sync()
        assert cur.sql("SELECT * FROM duckt ORDER BY id") == [1, 3, 4]
        assert cur.sql("SELECT * FROM heapt ORDER BY id") == []


def test_prepared_ctas(cur: Cursor):
    cur.sql("CREATE TABLE heapt (id int, number int)")
    cur.sql("INSERT INTO heapt VALUES (1, 2), (2, 4), (3, 6)")
    cur.sql("CREATE TEMP TABLE t USING duckdb AS SELECT * FROM heapt")
    assert cur.sql("SELECT * FROM t ORDER BY id") == [(1, 2), (2, 4), (3, 6)]

    # We don't support CTAS with parameters yet. The error message and code
    # could be better, but this is what we have right now. At least we don't
    # crash.
    with pytest.raises(
        psycopg.errors.InternalError,
        match="Could not find parameter with identifier 1",
    ):
        cur.sql(
            "CREATE TEMP TABLE t2 USING duckdb AS SELECT * FROM heapt where id = %s",
            (2,),
        )

    prepared_query = "CREATE TEMP TABLE t3 USING duckdb AS SELECT * FROM heapt"
    cur.sql(prepared_query, prepare=True)
    assert cur.sql("SELECT count(*) FROM t3") == 3
    cur.sql("DROP TABLE t3")
    cur.sql(prepared_query)
    assert cur.sql("SELECT count(*) FROM t3") == 3


def test_prepared_change_type(cur: Cursor, tmp_path):
    tmp_path = tmp_path / "test.csv"
    tmp_path.write_text("123\n")
    prepared_query = f"SELECT * FROM read_csv('{tmp_path}')"
    cur.sql(prepared_query, prepare=True)

    tmp_path.write_text("abc\n")
    with pytest.raises(
        psycopg.errors.InternalError,
        match="Types returned by duckdb query changed between planning and execution",
    ):
        cur.sql(prepared_query)

    tmp_path.write_text("1,234\n")
    with pytest.raises(
        psycopg.errors.InternalError,
        match="Number of columns returned by DuckDB query changed between planning and execution",
    ):
        cur.sql(prepared_query)
