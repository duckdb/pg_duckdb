from .utils import Cursor, PG_MAJOR_VERSION, eprint


def test_temporary_table(cur: Cursor):
    if PG_MAJOR_VERSION >= 15:
        cur.sql("CREATE TEMP TABLE t(a int)")
        cur.sql("ALTER TABLE t ADD COLUMN b int")

        try:
            cur.sql("ALTER TABLE t SET ACCESS METHOD heap")
        except Exception as err:
            eprint (err)

        cur.sql("CREATE INDEX ON t(a)")
        cur.sql("DROP TABLE t")
        cur.sql("CREATE TEMP TABLE t(a int) USING heap")
        cur.sql("ALTER TABLE t ADD COLUMN b int")

        try:
            cur.sql("ALTER TABLE t SET ACCESS METHOD duckdb")
        except Exception as err:
            eprint (err)
