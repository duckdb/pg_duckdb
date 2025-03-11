from .utils import Cursor, PG_MAJOR_VERSION

import pytest
import psycopg.errors


def test_temporary_table_alter_table(cur: Cursor):
    cur.sql("CREATE TEMP TABLE t(a int) USING duckdb")
    # We disallow most ALTER TABLE commands on duckdb tables
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        cur.sql("ALTER TABLE t FORCE ROW LEVEL SECURITY")

    if PG_MAJOR_VERSION >= 15:
        # We specifically want to disallow changing the access method
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            cur.sql("ALTER TABLE t SET ACCESS METHOD heap")

    with pytest.raises(psycopg.errors.FeatureNotSupported):
        cur.sql("CREATE INDEX ON t(a)")
    cur.sql("DROP TABLE t")
    cur.sql("CREATE TEMP TABLE t(a int) USING heap")
    # Check that we allow arbitrary ALTER TABLE commands on heap tables
    cur.sql("ALTER TABLE t FORCE ROW LEVEL SECURITY")

    if PG_MAJOR_VERSION >= 15:
        # We also don't want people to change the access method of a table to
        # duckdb after the table is created
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            cur.sql("ALTER TABLE t SET ACCESS METHOD duckdb")
