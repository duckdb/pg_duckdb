from .utils import Cursor, Postgres

import pytest
import psycopg.errors
import psycopg.sql


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
                "SELECT * FROM duckdb.raw_query($$ INSTALL avro FROM community; $$)"
            )

    # Even if such community extensions somehow get installed, it's not possible
    # to load them without changing allow_community_extensions. Not even for a
    # superuser.
    with pg.cur() as cur:
        cur.sql("SET duckdb.force_execution = false")
        cur.sql("SELECT * FROM duckdb.raw_query($$ INSTALL avro FROM community; $$)")
        with pytest.raises(
            Exception,
            match="IO Error: Extension .* could not be loaded because its signature is either missing or invalid and unsigned extensions are disabled by configuration",
        ):
            cur.sql("SELECT * FROM duckdb.raw_query($$ LOAD avro; $$)")

    # But it should be possible to load them after changing that setting.
    with pg.cur() as cur:
        cur.sql("SET duckdb.allow_community_extensions = true")
        cur.sql("SET duckdb.force_execution = false")
        cur.sql("SELECT * FROM duckdb.raw_query($$ LOAD avro; $$)")

    # And that setting is only changeable by superusers
    with pg.cur() as cur:
        cur.sql("SET ROLE user1")
        with pytest.raises(
            psycopg.errors.InsufficientPrivilege,
            match='permission denied to set parameter "duckdb.allow_community_extensions"',
        ):
            cur.sql("SET duckdb.allow_community_extensions = true")
