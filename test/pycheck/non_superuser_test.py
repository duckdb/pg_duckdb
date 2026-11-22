import psycopg.errors
import psycopg.sql
import pytest

from .utils import Postgres


def test_community_extensions(pg: Postgres):
    # The tests allow unsigned extensions for the httpfs of the DuckDB build,
    # this test needs the signature checks and not httpfs
    pg.sql("DELETE FROM duckdb.extensions WHERE name = 'httpfs'")
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
            cur.sql("SELECT * FROM duckdb.raw_query($$ INSTALL h3 FROM community; $$)")

    # Even if such community extensions somehow get installed, it's not possible
    # to load them without changing allow_community_extensions. Not even for a
    # superuser.
    with pg.cur() as cur:
        cur.sql("SET duckdb.force_execution = false")
        cur.sql("SET duckdb.allow_community_extensions = true")
        try:
            cur.sql("SELECT * FROM duckdb.raw_query($$ INSTALL h3 FROM community; $$)")
        except psycopg.errors.InternalError as e:
            # Community extensions are only published for DuckDB releases, not
            # for the development versions in between
            if "(HTTP 404)" in str(e):
                pytest.skip("h3 is not published for this DuckDB version")
            raise

    with pg.cur() as cur:
        cur.sql("SET duckdb.allow_community_extensions = false")
        cur.sql("SET duckdb.allow_unsigned_extensions = false")
        cur.sql("SET duckdb.force_execution = false")
        with pytest.raises(
            Exception,
            match="IO Error: Extension .* could not be loaded because its signature is either missing or invalid and unsigned extensions are disabled by configuration",
        ):
            cur.sql("SELECT * FROM duckdb.raw_query($$ LOAD h3; $$)")

    # But it should be possible to load them after changing that setting.
    with pg.cur() as cur:
        cur.sql("SET duckdb.allow_community_extensions = true")
        cur.sql("SET duckdb.allow_unsigned_extensions = false")
        cur.sql("SET duckdb.force_execution = false")
        cur.sql("SELECT * FROM duckdb.raw_query($$ LOAD h3; $$)")

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


def test_https_without_local_file_system(pg: Postgres):
    # Since DuckDB 2.0, httpfs reads the system CA certificate bundle through
    # the DuckDB file system. LocalFileSystem is disabled for roles without
    # pg_read_server_files, which must not stop them from reading https://
    # files (issue #1146). Nothing listens on port 1, so the request fails, but
    # httpfs loads the CA certificate bundle before it connects.
    pg.create_user("user1", psycopg.sql.SQL("IN ROLE duckdb_group"))
    url = "https://localhost:1/file.csv"
    with pg.cur() as cur:
        cur.sql("SET ROLE user1")
        with pytest.raises(psycopg.errors.InternalError) as error:
            cur.sql(f"SELECT * FROM read_csv('{url}')")
        assert "CA certificate bundle" not in str(error.value)
        assert "IO Error" in str(error.value) or "HTTP Error" in str(error.value)

        # Other local files stay blocked, also next to the CA certificate bundles
        for path in ["/etc/passwd", "/etc/ssl/cert.pem/../../passwd", "/etc/ssl/"]:
            with pytest.raises(
                psycopg.errors.InternalError,
                match="File system LocalFileSystem has been disabled by configuration",
            ):
                cur.sql(f"SELECT * FROM read_csv('{path}')")

    # The file system for the CA certificate bundles can be disabled too
    with pg.cur() as cur:
        cur.sql("SET duckdb.disabled_filesystems = 'PgDuckDBCACertificateFileSystem'")
        cur.sql("SET ROLE user1")
        with pytest.raises(
            psycopg.errors.InternalError,
            match="Cannot read a CA certificate bundle: file system access is restricted",
        ):
            cur.sql(f"SELECT * FROM read_csv('{url}')")
