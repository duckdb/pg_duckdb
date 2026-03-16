import psycopg.errors
import psycopg.sql
import pytest

from .utils import Postgres


def test_community_extensions(pg: Postgres):
    pg.create_user("user1", psycopg.sql.SQL("IN ROLE duckdb_group"))
    # Raw extension installation should not be possible non-superusers, because
    # that would allow installing extensions from community repos.
    with pg.cur() as cur:
        cur.sql("SET ROLE user1")
        print(cur.sql("SHOW ROLE"))
        cur.sql("SET duckdb.force_execution = false")
        with pytest.raises(
            psycopg.errors.InternalError,
            match="Permission Error: File system LocalFileSystem has been disabled by configuration",
        ):
            cur.sql(
                "SELECT * FROM duckdb.raw_query($$ INSTALL prql FROM community; $$)"
            )

    # Community extensions are unsigned by default and should not install unless
    # unsigned extensions are explicitly allowed.
    with pg.cur() as cur:
        cur.sql("SET duckdb.force_execution = false")
        with pytest.raises(
            psycopg.errors.InternalError,
            match="IO Error: Attempting to install an extension file that doesn't have a valid signature",
        ):
            cur.sql(
                "SELECT * FROM duckdb.raw_query($$ INSTALL prql FROM community; $$)"
            )

    # And that setting is only changeable by superusers
    with pg.cur() as cur:
        cur.sql("SET ROLE user1")
        with pytest.raises(
            psycopg.errors.InsufficientPrivilege,
            match='permission denied to set parameter "duckdb.allow_community_extensions"',
        ):
            cur.sql("SET duckdb.allow_community_extensions = true")


def test_install_extension_injection(pg: Postgres):
    pg.create_user("user1", psycopg.sql.SQL("IN ROLE duckdb_group"))
    with pg.cur() as cur:
        cur.sql("SET duckdb.force_execution = false")
        cur.sql("GRANT ALL ON FUNCTION duckdb.install_extension(TEXT, TEXT) TO user1;")
        cur.sql("SET ROLE user1")

        # Try with the install_extension function
        with pytest.raises(
            psycopg.errors.InternalError,
            match=r"""HTTP Error: Failed to download extension " '; select \* from hacky '' """,
        ):
            cur.sql(
                "SELECT * FROM duckdb.install_extension($$ '; select * from hacky '' $$);"
            )

    with pg.cur() as cur:
        cur.sql(
            "INSERT INTO duckdb.extensions (name) VALUES ($$ '; select * from hacky '' $$);"
        )
        with pytest.raises(
            psycopg.errors.InternalError,
            match=r"""HTTP Error: Failed to download extension " '; select \* from hacky '' """,
        ):
            cur.sql("SELECT * FROM duckdb.query($$ SELECT 1 $$)")
