"""Tests for MotherDuck

These tests are using Python because we want to test the table metadata
syncing, so we need to be able to use make changes to the metadata from a
different client that pg_duckdb. By using python we can use the DuckDB python
library for that purpose.
"""

import datetime
import duckdb
import uuid

from .utils import wait_until, Cursor, Postgres, Duckdb, PG_MAJOR_VERSION
from .motherduck_token_helper import (
    can_run_md_multi_user_tests,
    can_run_md_tests,
    create_read_scaling_token,
)
from .multi_duckdb_helper import MDClient
from contextlib import suppress

import pytest
import psycopg.errors


if not can_run_md_tests():
    pytestmark = pytest.mark.skip(reason="Skipping all motherduck tests")


def test_md_duckdb_version(ddb, md_cur: Cursor):
    version_query = "SELECT library_version FROM pragma_version();"
    python_duckdb_version = ddb.sql(version_query)
    pg_duckdb_duckdb_version = md_cur.sql(
        f"SELECT * FROM duckdb.query('{version_query}');"
    )

    assert python_duckdb_version == pg_duckdb_duckdb_version


def test_md_create_table(md_cur: Cursor, ddb):
    ddb.sql("CREATE TABLE t1(a int, b varchar)")
    ddb.sql("INSERT INTO t1 VALUES (1, 'abc')")
    md_cur.wait_until_table_exists("t1")

    assert md_cur.sql("SELECT * FROM t1") == (1, "abc")
    assert md_cur.sql(
        "SELECT attname, atttypid::regtype FROM pg_attribute WHERE attrelid = 't1'::regclass AND attnum > 0"
    ) == [
        ("a", "integer"),
        ("b", "text"),
    ]
    md_cur.sql("CREATE TABLE t2(a int) USING duckdb")
    md_cur.sql("INSERT INTO t2 VALUES (2)")
    assert md_cur.sql("SELECT * FROM t2") == 2
    assert ddb.sql("SELECT * FROM t2") == 2

    md_cur.sql("CREATE TABLE t3(a int)")
    md_cur.sql("INSERT INTO t3 VALUES (3)")
    assert md_cur.sql("SELECT * FROM t3") == 3
    assert ddb.sql("SELECT * FROM t3") == 3

    with pytest.raises(
        psycopg.errors.InvalidTableDefinition,
        match=r"Creating a non-DuckDB table in a ddb\$ schema is not supported",
    ):
        md_cur.sql("CREATE TABLE t4(a int) USING heap")


def test_md_default_db_escape(pg: Postgres, ddb, default_db_name, md_test_user):
    # Make sure MD is not enabled
    pg.sql("DROP SERVER IF EXISTS motherduck CASCADE;")

    weird_db_name = "some 19 really $  - @ weird name 😀 84"
    ddb.sql(f'DROP DATABASE IF EXISTS "{weird_db_name}";')
    ddb.sql(f'CREATE DATABASE "{weird_db_name}";')
    pg.search_path = f"ddb${default_db_name}, public"
    with pg.cur() as cur:
        cur.execute(
            f"CALL duckdb.enable_motherduck('{md_test_user['token']}', '{weird_db_name}')"
        )
        cur.wait_until_schema_exists(f"ddb${default_db_name}")

        # Make sure DuckDB is using the provided session hint
        assert (
            cur.sql("SELECT * FROM duckdb.query($$ SELECT current_database(); $$);")
            == weird_db_name
        )

        assert pg.sql("""
            SELECT fs.srvname, fs.srvtype, fs.srvoptions
            FROM pg_foreign_server fs
            INNER JOIN pg_foreign_data_wrapper fdw ON fdw.oid = fs.srvfdw
            LEFT JOIN pg_user_mapping um ON um.umserver = fs.oid
            WHERE fdw.fdwname = 'duckdb' AND fs.srvtype = 'motherduck';
        """) == ("motherduck", "motherduck", [f"default_database={weird_db_name}"])


def test_md_session_hint(pg: Postgres, ddb, default_db_name, md_test_user):
    pg.search_path = f"ddb${default_db_name}, public"

    # Make sure MD is not enabled
    pg.sql("DROP SERVER IF EXISTS motherduck CASCADE;")
    with pg.cur() as cur:
        hint = "abc123"
        cur.sql(f"SET duckdb.motherduck_session_hint = '{hint}';")

        # Sanity check
        assert cur.sql("SHOW duckdb.motherduck_session_hint;") == hint

        cur.execute(f"CALL duckdb.enable_motherduck('{md_test_user['token']}')")
        cur.wait_until_schema_exists(f"ddb${default_db_name}")

        # Make sure DuckDB is using the provided session hint
        assert (
            cur.sql(
                "SELECT * FROM duckdb.query($$ SELECT current_setting('motherduck_session_hint'); $$);"
            )
            == hint
        )


@pytest.mark.skipif(not can_run_md_multi_user_tests(), reason="needs multiple users")
def test_md_multiple_databases(pg_two_dbs):
    cur1, cur2 = pg_two_dbs
    with MDClient.create("test_md_db_1", "test_md_db_2") as (cli1, cli2):
        # Connect each user in PG to their respective DBs
        cur1.sql(f"CALL duckdb.enable_motherduck('{cli1.get_token()}', 'test_md_db_1')")
        cur2.sql(f"CALL duckdb.enable_motherduck('{cli2.get_token()}', 'test_md_db_2')")

        # Create a table in user1's DB...
        cli1.run_query("CREATE TABLE user1_t1(a int, b varchar)")
        cli1.run_query("INSERT INTO user1_t1 VALUES (41, 'hello user1')")

        # And make sure we can read it.
        cur1.wait_until_table_exists("user1_t1")
        assert cur1.sql("SELECT * FROM user1_t1") == (41, "hello user1")

        # Same for user2
        cli2.run_query("CREATE TABLE user2_t1(a int, b varchar)")
        cli2.run_query("INSERT INTO user2_t1 VALUES (42, 'hello user2')")

        cur2.wait_until_table_exists("user2_t1")
        assert cur2.sql("SELECT * FROM user2_t1") == (42, "hello user2")

        # Make sure each user can only see their own tables
        list_tables_query = """
        SELECT table_schema,table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema', 'duckdb')
          AND table_schema NOT LIKE 'ddb$sample_data%';
        """
        assert cur1.sql(list_tables_query) == ("public", "user1_t1")
        assert cur2.sql(list_tables_query) == ("public", "user2_t1")


@pytest.mark.timeout(120)
@pytest.mark.skipif(not can_run_md_multi_user_tests(), reason="needs multiple users")
def test_md_read_scaling_two_connections(pg_two_dbs, md_test_user):
    cur1, cur2 = pg_two_dbs
    ## FIXME - If the DB is not unique, the test fails
    db_name = "test_md_db" + str(uuid.uuid4()).replace("-", "")
    user1_spec = {"database": db_name, "token": md_test_user["token"], "hint": "cli1"}
    user2_spec = {
        "database": db_name,
        "token": create_read_scaling_token(md_test_user)["token"],
        "reset_db": False,
        "hint": "cli2",
    }

    with MDClient.create(user1_spec) as cli1:
        # Create a table in user1's DB...
        cli1.run_query("DROP TABLE IF EXISTS t1;")
        cli1.run_query("CREATE TABLE t1(a int, b varchar)")
        cli1.run_query("INSERT INTO t1 VALUES (41, 'hello world')")

        with MDClient.create(user2_spec) as cli2:
            # First check in the second connection that we can see the table
            for _ in wait_until("Failed to get t1 records", timeout=70):
                with suppress(duckdb.duckdb.CatalogException):
                    cli2.run_query(f"REFRESH DATABASE {db_name};")
                    if cli2.run_query("SELECT * FROM t1") == [(41, "hello world")]:
                        break

            # Connect each user in PG to their respective DBs
            cur1.sql(
                f"CALL duckdb.enable_motherduck('{cli1.get_token()}', '{db_name}')"
            )
            cur2.sql(
                f"CALL duckdb.enable_motherduck('{cli2.get_token()}', '{db_name}')"
            )

            cur1.wait_until_table_exists("t1", timeout=70)
            assert cur1.sql("SELECT * FROM t1") == (41, "hello world")

            cur2.wait_until_table_exists("t1", timeout=70)
            assert cur2.sql("SELECT * FROM t1") == (41, "hello world")

            with pytest.raises(
                psycopg.errors.InternalError_,
                match=r"Cannot execute statement of type \"INSERT\" on database \".*\" which is attached in read-only mode!",
            ):
                cur2.sql("INSERT INTO t1 VALUES (42, 'nope')")


def test_md_ctas(md_cur: Cursor, ddb):
    ddb.sql("CREATE TABLE t1 AS SELECT 1 a")
    md_cur.wait_until_table_exists("t1")

    assert md_cur.sql("SELECT * FROM t1") == 1
    md_cur.sql("CREATE TABLE t2(b) USING duckdb AS SELECT 2 a")
    assert md_cur.sql("SELECT * FROM t2") == 2
    assert ddb.sql("SELECT * FROM t2") == 2

    md_cur.sql("CREATE TABLE t3(b) AS SELECT 3 a")
    assert md_cur.sql("SELECT * FROM t3") == 3
    assert ddb.sql("SELECT * FROM t3") == 3

    with pytest.raises(
        psycopg.errors.InvalidTableDefinition,
        match=r"Creating a non-DuckDB table in a ddb\$ schema is not supported",
    ):
        md_cur.sql("CREATE TABLE t4(b) USING heap AS SELECT 4 a")


def test_md_alter_table(md_cur: Cursor):
    md_cur.sql("CREATE TABLE t(a) USING duckdb AS SELECT 1")
    # We disallow most ALTER TABLE commands on duckdb tables
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        md_cur.sql("ALTER TABLE t FORCE ROW LEVEL SECURITY")

    with pytest.raises(
        psycopg.errors.FeatureNotSupported,
        match="Changing the schema of a duckdb table is currently not supported",
    ):
        md_cur.sql("ALTER TABLE t SET SCHEMA public")

    md_cur.sql("ALTER TABLE t ADD COLUMN b int DEFAULT 100")
    for _ in wait_until("Failed to add column"):
        with suppress(duckdb.duckdb.CatalogException):
            if md_cur.sql("SELECT * FROM t") == (1, 100):
                break

    if PG_MAJOR_VERSION >= 15:
        # We specifically want to disallow changing the access method
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            md_cur.sql("ALTER TABLE t SET ACCESS METHOD heap")

    with pytest.raises(psycopg.errors.FeatureNotSupported):
        md_cur.sql("CREATE INDEX ON t(a)")
    md_cur.sql("DROP TABLE t")
    md_cur.sql("CREATE TABLE public.t(a int) USING heap")
    # Check that we allow arbitrary ALTER TABLE commands on heap tables
    md_cur.sql("ALTER TABLE t FORCE ROW LEVEL SECURITY")

    if PG_MAJOR_VERSION >= 15:
        # We also don't want people to change the access method of a table to
        # duckdb after the table is created
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            md_cur.sql("ALTER TABLE t SET ACCESS METHOD duckdb")


def test_md_duckdb_only_types(md_cur: Cursor, ddb: Duckdb):
    ddb.sql("""
            CREATE TABLE t1(
                m MAP(INT, VARCHAR),
                s STRUCT(v VARCHAR, i INTEGER),
                u UNION(t time, d date),
            )""")
    ddb.sql("""
        INSERT INTO t1 VALUES (
            MAP{1: 'abc'},
            {'v': 'struct abc', 'i': 123},
            '12:00'::time,
        ), (
            MAP{2: 'def'},
            {'v': 'struct def', 'i': 456},
            '2023-10-01'::date,
        )
    """)
    md_cur.wait_until_table_exists("t1")
    assert md_cur.sql("""select * from t1""") == [
        ("{1=abc}", "{'v': struct abc, 'i': 123}", "12:00:00"),
        ("{2=def}", "{'v': struct def, 'i': 456}", "2023-10-01"),
    ]

    assert md_cur.sql("""select m[1] from t1""") == ["abc", None]
    assert md_cur.sql("""select s['v'] from t1""") == ["struct abc", "struct def"]
    assert md_cur.sql("""select s['i'] from t1""") == [123, 456]
    assert md_cur.sql("""select union_extract(u,'t') from t1""") == [
        datetime.time(12, 0),
        None,
    ]
    assert md_cur.sql("""select union_extract(u, 'd') from t1""") == [
        None,
        datetime.date(2023, 10, 1),
    ]
    assert md_cur.sql("""select union_tag(u) from t1""") == ["t", "d"]
