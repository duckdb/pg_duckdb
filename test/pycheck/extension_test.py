import shutil

import psycopg.errors
import psycopg.sql
import pytest

from .utils import Cursor, Postgres


def test_autoinstall_known_extensions(pg: Postgres, cur: Cursor):
    cur.sql("SET duckdb.autoinstall_known_extensions = 'off'")
    # Cleanup any existing extensions before the test
    extension_dir = pg.pgdata / "pg_duckdb" / "extensions"
    if extension_dir.exists():
        shutil.rmtree(extension_dir)
    cur.sql("TRUNCATE duckdb.extensions")
    # excel is a known core extension, so it's listed even when not installed
    assert cur.sql(
        "SELECT * FROM duckdb.query($$ FROM duckdb_extensions() SELECT installed, loaded WHERE extension_name = 'excel' $$)"
    ) == (False, False)

    cur.sql("SELECT duckdb.install_extension('excel')")
    assert cur.sql(
        "SELECT * FROM duckdb.query($$ FROM duckdb_extensions() SELECT installed, loaded WHERE extension_name = 'excel' $$)"
    ) == (True, True)

    # Now we delete it from disk, which should be reflected in the duckdb_extensions() table function
    if extension_dir.exists():
        shutil.rmtree(extension_dir)
    assert cur.sql(
        "SELECT * FROM duckdb.query($$ FROM duckdb_extensions() SELECT installed, loaded WHERE extension_name = 'excel' $$)"
    ) == (False, True)

    # We close the duckdb database for this session.
    cur.sql("CALL duckdb.recycle_ddb()")
    # The next query should then try to load the extension, which will fail because it's not there and autoinstall_known_extensions is off.
    with pytest.raises(
        psycopg.errors.InternalError,
        match=r'excel.duckdb_extension" not found.',
    ):
        cur.sql("SELECT * FROM duckdb.query($$ SELECT 1 $$)")

    # Let's try that again with autoinstall_known_extensions on. That should make it work.
    cur.sql("CALL duckdb.recycle_ddb()")
    cur.sql("SET duckdb.autoinstall_known_extensions = 'on'")
    assert cur.sql(
        "SELECT * FROM duckdb.query($$ FROM duckdb_extensions() SELECT installed, loaded WHERE extension_name = 'excel' $$)"
    ) == (True, True)
    assert (
        cur.sql(
            "SELECT * FROM duckdb.query($$ SELECT excel_text(1_234_567.897, 'h:mm AM/PM') $$)"
        )
        == "9:31 PM"
    )

    # MotherDuck should also not be installed automatically if
    # autoinstall_known_extensions is off
    cur.sql("CALL duckdb.recycle_ddb()")
    cur.sql("SET duckdb.autoinstall_known_extensions = 'off'")
    # Disabling duckdb.autoinstall_known_extensions should prevent
    # automatically installing the MotherDuck extension.
    pg.create_server(
        "motherduck",
        psycopg.sql.SQL("TYPE 'motherduck' FOREIGN DATA WRAPPER duckdb"),
    )

    cur.sql(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER motherduck OPTIONS (token 'fake-token')"
    )

    with pytest.raises(
        psycopg.errors.InternalError,
        match=r'motherduck.duckdb_extension" not found.',
    ):
        cur.sql("SELECT * FROM duckdb.query($$ SELECT 1 $$)")
