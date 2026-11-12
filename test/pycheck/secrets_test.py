import os
import re
import threading
from contextlib import suppress

import psycopg.errors
import pytest

from .utils import Postgres, wait_until

# Every fake secret value contains this marker, so we can check that none of
# them ends up in the log or in pg_stat_statements.
SECRET_MARKER = "FAKE_SECRET_VALUE"
REDACTED = " <redacted by pg_duckdb because it may contain secrets, see duckdb.allow_unredacted_secrets option>"


def drop_secret_servers(pg: Postgres):
    pg.sql("DROP SERVER IF EXISTS simple_s3_secret CASCADE")
    pg.sql("DROP SERVER IF EXISTS simple_gcs_secret CASCADE")


def assert_no_secrets(text):
    leaked = re.findall(rf".*{SECRET_MARKER}.*", text)
    assert not leaked, "secrets leaked:\n" + "\n".join(leaked)


def test_secrets_are_redacted_in_log(pg: Postgres):
    pg.create_user("secret_user")
    try:
        with pg.log_path.open() as log:
            log.seek(0, os.SEEK_END)

            with pg.cur() as cur:
                cur.sql("SET log_statement = 'all'")
                # Successful statements, which log_statement logs
                cur.sql(f"""
                    SELECT duckdb.create_simple_secret(
                        type := 'S3',
                        key_id := 'key_{SECRET_MARKER}_1',
                        secret => 'secret_{SECRET_MARKER}_1',
                        session_token := 'token_{SECRET_MARKER}_1',
                        region := 'us-east-1')
                """)
                # Bound parameters, which are logged in the DETAIL
                cur.sql(
                    "SELECT duckdb.create_simple_secret(%s, %s, %s)",
                    ("GCS", f"key_{SECRET_MARKER}_2", f"secret_{SECRET_MARKER}_2"),
                )
                cur.sql(f"""
                    ALTER USER MAPPING FOR CURRENT_USER SERVER simple_s3_secret
                    OPTIONS (SET key_id 'key_{SECRET_MARKER}_3', SET "secret" E'it''s \\' a {SECRET_MARKER}_3')
                """)
                cur.sql("RESET log_statement")

                # Failed statements are logged with their STATEMENT
                with pytest.raises(psycopg.errors.UndefinedObject):
                    cur.sql(f"""
                        ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server
                        OPTIONS (SET key_id 'key_{SECRET_MARKER}_4', ADD secret $x${SECRET_MARKER}_4$x$)
                    """)
                with pytest.raises(psycopg.errors.SyntaxError):
                    cur.sql(
                        f"CREATE USER MAPING FOR CURRENT_USER SERVER simple_s3_secret OPTIONS (secret 'secret_{SECRET_MARKER}_5')"
                    )
                with pytest.raises(psycopg.errors.SyntaxError):
                    cur.sql(
                        f"SELECT duckdb.create_simple_secret('S3', 'key_{SECRET_MARKER}_6', 'secret_{SECRET_MARKER}_6"
                    )

                cur.sql("SET ROLE secret_user")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.sql(
                        f"SELECT duckdb.create_simple_secret('S3', 'key_{SECRET_MARKER}_7', 'secret_{SECRET_MARKER}_7')"
                    )
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.sql(
                        f"SELECT duckdb.create_azure_secret('AccountKey={SECRET_MARKER}_8')"
                    )
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.sql(f"CALL duckdb.enable_motherduck('token_{SECRET_MARKER}_9')")
                cur.sql("RESET ROLE")

                # Errors in an aborted transaction block
                cur.sql("BEGIN")
                with pytest.raises(psycopg.errors.DivisionByZero):
                    cur.sql("SELECT 1/0")
                with pytest.raises(psycopg.errors.InFailedSqlTransaction):
                    cur.sql(
                        f"ALTER USER MAPPING FOR CURRENT_USER SERVER simple_s3_secret OPTIONS (SET key_id 'key_{SECRET_MARKER}_10')"
                    )
                cur.sql("ROLLBACK")

            with pg.cur() as cur:
                # Several statements in one query
                cur.sql("SET log_statement = 'all'")
                cur.sql(
                    f"ALTER USER MAPPING FOR CURRENT_USER SERVER simple_gcs_secret OPTIONS (SET key_id 'key_{SECRET_MARKER}_11'); "
                    "SELECT 'not a secret'"
                )
                cur.sql("RESET log_statement")

                cur.sql("SET log_min_duration_statement = 0")
                cur.sql(
                    f"ALTER USER MAPPING FOR CURRENT_USER SERVER simple_gcs_secret OPTIONS (SET key_id 'key_{SECRET_MARKER}_12')"
                )
                cur.sql("RESET log_min_duration_statement")

                # Secrets in a query run with SPI end up in the error context
                cur.sql(f"SET pg_duckdb_test.secret = 'key_{SECRET_MARKER}_13'")
                with pytest.raises(psycopg.errors.UndefinedObject) as error:
                    cur.sql("""
                        DO $$ BEGIN
                        EXECUTE 'ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server OPTIONS (SET key_id '
                            || quote_literal(current_setting('pg_duckdb_test.secret')) || ')';
                        END $$
                    """)
                assert SECRET_MARKER not in str(error.value.diag.context)
                assert 'SQL statement "ALTER USER MAPPING' + REDACTED in str(
                    error.value.diag.context
                )

            logs = log.read()

        assert_no_secrets(logs)
        # The statements are logged up to the first word that hints at a secret
        assert "SELECT duckdb.create_simple_secret" + REDACTED in logs
        assert (
            "execute <unnamed>: SELECT duckdb.create_simple_secret" + REDACTED in logs
        )
        assert "DETAIL:  " + REDACTED[1:] in logs
        assert (
            "STATEMENT:  \n\t                        ALTER USER MAPPING" + REDACTED
            in logs
        )
        assert 'server "no_such_server" does not exist' in logs
        assert (
            "CREATE USER MAPING FOR CURRENT_USER SERVER simple_s3_secret" + REDACTED
            in logs
        )
        assert "unterminated quoted string at or near \"'secret" + REDACTED in logs
        assert "SELECT duckdb.create_azure_secret" + REDACTED in logs
        assert "CALL duckdb.enable_motherduck" + REDACTED in logs
        assert "statement: ALTER USER MAPPING" + REDACTED in logs
        assert re.search(r"duration: .* statement: ALTER USER MAPPING <redacted", logs)
        # Other statements are logged as is
        assert "STATEMENT:  SELECT 1/0" in logs
    finally:
        drop_secret_servers(pg)


def test_allow_unredacted_secrets(pg: Postgres):
    pg.create_user("secret_user")
    unredacted = "UNREDACTED_VALUE"
    show_secrets = "SELECT * FROM duckdb.query($$ SELECT secret_string FROM duckdb_secrets(redact := false) $$)"
    try:
        with pg.log_path.open() as log:
            log.seek(0, os.SEEK_END)
            with pg.cur() as cur:
                cur.sql("SET duckdb.allow_unredacted_secrets = true")
                with pytest.raises(psycopg.errors.UndefinedObject):
                    cur.sql(
                        f"ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server OPTIONS (SET secret '{unredacted}_1')"
                    )

                # DuckDB gets the setting too, so it shows unredacted secrets
                cur.sql(
                    f"SELECT duckdb.create_simple_secret('S3', 'key', '{unredacted}_2')"
                )
                assert f"{unredacted}_2" in str(cur.sql(show_secrets))

                # Like other DuckDB settings, it cannot be changed once DuckDB is initialized
                with pytest.raises(
                    psycopg.errors.InvalidParameterValue,
                    match="Cannot set this variable after DuckDB has been initialized",
                ):
                    cur.sql("SET duckdb.allow_unredacted_secrets = false")

            with pg.cur() as cur:
                with pytest.raises(
                    psycopg.errors.Error,
                    match="Displaying unredacted secrets is disabled",
                ):
                    cur.sql(show_secrets)

            with pg.cur() as cur:
                with pytest.raises(psycopg.errors.UndefinedObject):
                    cur.sql(
                        f"ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server OPTIONS (SET secret '{unredacted}_3')"
                    )

                # Only superusers can disable the redaction
                cur.sql("SET ROLE secret_user")
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    cur.sql("SET duckdb.allow_unredacted_secrets = true")
                cur.sql("RESET ROLE")
            logs = log.read()

        assert f"OPTIONS (SET secret '{unredacted}_1')" in logs
        assert f"{unredacted}_3" not in logs
        assert "ALTER USER MAPPING" + REDACTED in logs
    finally:
        drop_secret_servers(pg)


def test_secrets_of_other_sessions_are_redacted_in_deadlock_report(pg: Postgres):
    """A deadlock report lists the statements of all sessions in its detail_log"""
    pg.sql("CREATE TABLE deadlock_t (id int PRIMARY KEY, v text)")
    pg.sql("INSERT INTO deadlock_t VALUES (1, ''), (2, '')")
    with pg.log_path.open() as log:
        log.seek(0, os.SEEK_END)
        with pg.cur() as a, pg.cur() as b:
            # Session a detects the deadlock, so the statement of b is only in the report
            a.sql("SET duckdb.force_execution = false")
            a.sql("SET deadlock_timeout = '100ms'")
            b.sql("SET duckdb.force_execution = false")
            b.sql("SET deadlock_timeout = '1min'")
            a.sql("BEGIN")
            a.sql("UPDATE deadlock_t SET v = 'a' WHERE id = 1")
            b.sql("BEGIN")
            b.sql("UPDATE deadlock_t SET v = 'b' WHERE id = 2")
            b_pid = b.sql("SELECT pg_backend_pid()")

            blocked = threading.Thread(
                target=b.sql,
                args=(
                    f"UPDATE deadlock_t /* secret */ SET v = 'secret_{SECRET_MARKER}' WHERE id = 1",
                ),
            )
            blocked.start()
            for _ in wait_until("session b is not waiting for the lock"):
                if pg.sql(
                    "SELECT wait_event_type = 'Lock' FROM pg_stat_activity WHERE pid = %s",
                    (b_pid,),
                ):
                    break

            with pytest.raises(psycopg.errors.DeadlockDetected):
                a.sql("UPDATE deadlock_t SET v = 'a' WHERE id = 2")
            a.sql("ROLLBACK")
            blocked.join()
            b.sql("ROLLBACK")
        logs = log.read()

    assert "deadlock detected" in logs
    assert_no_secrets(logs)
    assert f"Process {b_pid}: UPDATE deadlock_t /* secret" + REDACTED in logs


def test_do_block_quoted_as_string_is_redacted(pg: Postgres):
    do_block = (
        "DO 'BEGIN ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server "
        f"OPTIONS (SET secret ''secret_{SECRET_MARKER}''); END'"
    )
    with pg.log_path.open() as log:
        log.seek(0, os.SEEK_END)
        with pg.cur() as cur:
            with pytest.raises(psycopg.errors.UndefinedObject):
                cur.sql(do_block)
            with pytest.raises(psycopg.errors.UndefinedObject):
                cur.sql("SELECT 1; /* comment */ " + do_block)
            # Other strings are no code, so they are kept
            with pytest.raises(psycopg.errors.DivisionByZero):
                cur.sql("SELECT 'my secret', 1/0")
        logs = log.read()

    assert_no_secrets(logs)
    assert "STATEMENT:  DO 'BEGIN ALTER USER MAPPING" + REDACTED in logs
    assert (
        "STATEMENT:  SELECT 1; /* comment */ DO 'BEGIN ALTER USER MAPPING" + REDACTED
        in logs
    )
    assert "STATEMENT:  SELECT 'my secret', 1/0" in logs


def test_code_in_strings_is_redacted(pg: Postgres):
    pg.create_user("secret_user")
    mapping = f"ALTER USER MAPPING FOR CURRENT_USER SERVER no_such_server OPTIONS (SET secret '{SECRET_MARKER}')"
    quoted_mapping = mapping.replace("'", "''")
    with pg.log_path.open() as log:
        log.seek(0, os.SEEK_END)
        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = false")
            cur.sql("SET log_statement = 'all'")
            # Function bodies, quoted in both ways
            cur.sql(
                "CREATE FUNCTION f1() RETURNS void LANGUAGE plpgsql "
                f"AS 'BEGIN {quoted_mapping}; END'"
            )
            cur.sql(
                "CREATE FUNCTION f2() RETURNS void LANGUAGE plpgsql "
                f"AS $body$ BEGIN {mapping}; END $body$"
            )
            # Queries that are passed to DuckDB
            cur.sql(
                "SELECT duckdb.raw_query(query := 'CREATE SECRET s1 "
                f"(TYPE s3, KEY_ID ''{SECRET_MARKER}'', SECRET ''x'')')"
            )
            cur.sql(
                "SELECT * FROM duckdb.query('SELECT name FROM duckdb_secrets() "
                f"WHERE name = ''{SECRET_MARKER}''')"
            )
            # Passwords, which DuckDB may reject, but they are logged before
            with suppress(psycopg.errors.Error):
                cur.sql(
                    f"SELECT duckdb.raw_query($$ SET http_proxy_password = '{SECRET_MARKER}' $$)"
                )
            # As a user without access to DuckDB, so that DuckDB doesn't run it,
            # as its connection error would quote the password
            cur.sql("SET ROLE secret_user")
            with suppress(psycopg.errors.Error):
                cur.sql(
                    "SELECT duckdb.raw_query($$ ATTACH "
                    f"'host=localhost password={SECRET_MARKER}' AS db (TYPE postgres) $$)"
                )
            cur.sql("RESET ROLE")
            # Other strings are no code, so they are kept, also when quoted with $$
            cur.sql("SELECT $$my secret$$")
            cur.sql("RESET log_statement")
        logs = log.read()

    assert_no_secrets(logs)
    assert "AS 'BEGIN ALTER USER MAPPING" + REDACTED in logs
    assert "AS $body$ BEGIN ALTER USER MAPPING" + REDACTED in logs
    assert "SELECT duckdb.raw_query(query := 'CREATE SECRET" + REDACTED in logs
    assert "duckdb.query('SELECT name FROM duckdb_secret" + REDACTED in logs
    assert "SET http_proxy_password" + REDACTED in logs
    assert "ATTACH 'host=localhost password" + REDACTED in logs
    assert "statement: SELECT $$my secret$$" in logs


@pytest.mark.parametrize(
    "preload_libraries",
    ["pg_duckdb,pg_stat_statements", "pg_stat_statements,pg_duckdb"],
)
def test_secrets_not_in_pg_stat_statements(pg: Postgres, preload_libraries):
    if not pg.sql(
        "SELECT count(*) FROM pg_available_extensions WHERE name = 'pg_stat_statements'"
    ):
        pytest.skip("pg_stat_statements is not installed")

    pg.configure(
        f"shared_preload_libraries = '{preload_libraries}'\n"
        # Also track the USER MAPPING statements that the helpers run with SPI
        "pg_stat_statements.track = 'all'\n"
    )
    pg.restart()
    pg.sql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    try:
        pg.sql("SELECT pg_stat_statements_reset()")
        pg.sql(f"""
            SELECT duckdb.create_simple_secret(
                type := 'S3', key_id := 'key_{SECRET_MARKER}_1', secret := 'secret_{SECRET_MARKER}_1')
        """)
        pg.sql(
            f"ALTER USER MAPPING FOR CURRENT_USER SERVER simple_s3_secret OPTIONS (SET key_id 'key_{SECRET_MARKER}_2')"
        )
        pg.sql("CREATE SERVER simple_gcs_secret TYPE 'gcs' FOREIGN DATA WRAPPER duckdb")
        pg.sql(
            f"CREATE USER MAPPING FOR CURRENT_USER SERVER simple_gcs_secret OPTIONS (secret 'secret_{SECRET_MARKER}_3')"
        )
        # With duckdb.allow_unredacted_secrets the statements are tracked as is
        with pg.cur() as cur:
            cur.sql("SET duckdb.allow_unredacted_secrets = true")
            cur.sql(
                "ALTER USER MAPPING FOR CURRENT_USER SERVER simple_gcs_secret OPTIONS (SET secret 'UNREDACTED_VALUE')"
            )

        queries = pg.sql("SELECT string_agg(query, E'\\n') FROM pg_stat_statements")
        assert_no_secrets(queries)
        assert "OPTIONS (SET secret 'UNREDACTED_VALUE')" in queries
        # Other utility statements are still tracked
        assert "CREATE SERVER simple_gcs_secret" in queries
        assert "create_simple_secret" in queries
    finally:
        drop_secret_servers(pg)
        pg.sql("DROP EXTENSION pg_stat_statements")
