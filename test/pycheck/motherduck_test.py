"""Tests for MotherDuck

These tests are using Python because we want to test the table metadata
syncing, so we need to be able to use make changes to the metadata from a
different client that pg_duckdb. By using python we can use the DuckDB python
library for that purpose.
"""

from .utils import Cursor, MOTHERDUCK, PG_MAJOR_VERSION

import pytest
import psycopg.errors


if not MOTHERDUCK:
    pytestmark = pytest.mark.skip(reason="Skipping all motherduck tests")


def test_md_duckdb_version(md_cur: Cursor, ddb):
    version_query = "SELECT library_version FROM pragma_version();"
    python_duckdb_version = ddb.sql(version_query)
    pg_duckdb_duckdb_version = md_cur.sql(
        f"SELECT * FROM duckdb.query('{version_query}');"
    )

    assert python_duckdb_version == pg_duckdb_duckdb_version


def test_md_create_table(md_cur: Cursor, ddb):
    ddb.sql("CREATE TABLE t1(a int)")
    ddb.sql("INSERT INTO t1 VALUES (1)")
    md_cur.wait_until_table_exists("t1")

    assert md_cur.sql("SELECT * FROM t1") == 1
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

    md_cur.sql("ALTER TABLE t ADD COLUMN b int DEFAULT 100")
    assert md_cur.sql("SELECT * FROM t") == (1, 100)

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
