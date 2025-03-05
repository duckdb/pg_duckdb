"""Tests for MotherDuck

These tests are using Python because we want to test the table metadata
syncing, so we need to be able to use make changes to the metadata from a
different client that pg_duckdb. By using python we can use the DuckDB python
library for that purpose.
"""

from .utils import Cursor, MOTHERDUCK

import pytest
import psycopg.errors


if not MOTHERDUCK:
    pytestmark = pytest.mark.skip(reason="Skipping all motherduck tests")


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
    ddb.execute("CREATE TABLE t1 AS SELECT 1 a")
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
