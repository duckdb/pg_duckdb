import os
import pytest

import duckdb

from .utils import Postgres, Duckdb
from .motherduck_token_helper import create_test_user


@pytest.fixture(scope="session")
def shared_pg(tmp_path_factory):
    """Starts a new Postgres db that is shared for tests in this process"""
    pg = Postgres(tmp_path_factory.getbasetemp() / "pgdata")
    pg.initdb()

    pg.start()
    pg.sql("CREATE ROLE duckdb_group")
    pg.sql("GRANT CREATE ON SCHEMA public TO duckdb_group")
    pg.sql("CREATE EXTENSION pg_duckdb")

    yield pg

    pg.cleanup()


@pytest.fixture
def default_db_name(request):
    """Returns the name of the database used by the test"""
    yield request.node.name.removeprefix("test_")


def create_duckdb(db_name, token):
    con_string = f"md:?token={token}"
    con = duckdb.connect(con_string)
    con.execute(f"DROP DATABASE IF EXISTS {db_name}")
    con.execute(f"CREATE DATABASE {db_name}")
    con.execute(f"USE {db_name}")
    return con


@pytest.fixture
def pg(shared_pg, default_db_name):
    """
    Wraps the shared_pg fixture to reset the db after each test.

    It also creates a schema for the test to use. And logs the pg log to stdout
    for debugging failures.
    """
    shared_pg.reset()

    with shared_pg.log_path.open() as f:
        f.seek(0, os.SEEK_END)
        try:
            test_schema_name = default_db_name
            shared_pg.create_schema(test_schema_name)
            shared_pg.search_path = f"{test_schema_name}, public"
            yield shared_pg
        finally:
            try:
                shared_pg.cleanup_test_leftovers()
            finally:
                print("\n\nPG_LOG\n")
                print(f.read())


@pytest.fixture
def cur(pg):
    with pg.cur() as cur:
        yield cur


@pytest.fixture
def conn(pg):
    with pg.conn() as conn:
        yield conn


@pytest.fixture(scope="session")
def md_test_user():
    """Returns the test user token for MotherDuck.

    This makes sure that it's the same in all the places we use it
    """
    return create_test_user()


@pytest.fixture
def md_cur(pg, default_db_name, ddb, md_test_user):
    """A cursor to a MotherDuck enabled pg_duckdb"""
    # We don't actually need to use ddb connection, but we include the
    # fixture to make sure that the test database for the test is
    # dropped+created
    _ = ddb

    pg.sql(f"CALL duckdb.enable_motherduck('{md_test_user['token']}')")

    pg.search_path = f"ddb${default_db_name}, public"
    with pg.cur() as cur:
        cur.wait_until_schema_exists(f"ddb${default_db_name}")
        yield cur


@pytest.fixture
def ddb(default_db_name, md_test_user):
    """A DuckDB connection to MotherDuck

    This also creates a database for the test to use.
    """
    ddb_con = create_duckdb(default_db_name, md_test_user["token"])

    try:
        yield Duckdb(ddb_con)
    finally:
        ddb_con.execute("USE my_db")
        ddb_con.execute(f"DROP DATABASE {default_db_name}")
