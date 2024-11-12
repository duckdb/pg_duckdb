from .utils import Cursor, PG_MAJOR_VERSION

import pytest
import psycopg.errors


def test_temporary_table_alter_table(cur: Cursor):
    cur.sql("CREATE TEMP TABLE t(a int) USING duckdb")
    with pytest.raises(psycopg.errors.InternalError_):
        cur.sql("ALTER TABLE t ADD COLUMN b int")

    if PG_MAJOR_VERSION >= 15:
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            cur.sql("ALTER TABLE t SET ACCESS METHOD heap")

    with pytest.raises(psycopg.errors.FeatureNotSupported):
        cur.sql("CREATE INDEX ON t(a)")
    cur.sql("DROP TABLE t")
    cur.sql("CREATE TEMP TABLE t(a int) USING heap")
    cur.sql("ALTER TABLE t ADD COLUMN b int")

    if PG_MAJOR_VERSION >= 15:
        with pytest.raises(psycopg.errors.FeatureNotSupported):
            cur.sql("ALTER TABLE t SET ACCESS METHOD duckdb")
