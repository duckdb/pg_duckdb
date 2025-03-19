import os
import pytest

import duckdb

from .utils import Postgres, Duckdb


@pytest.fixture(scope="session")
def shared_pg(tmp_path_factory):
    """Starts a new Postgres db that is shared for tests in this process"""
    pg = Postgres(tmp_path_factory.getbasetemp() / "pgdata")
    pg.initdb()

    pg.start()
    pg.sql("CREATE ROLE duckdb_group")
    pg.sql("GRANT CREATE ON SCHEMA public TO duckdb_group")
    pg.sql("create extension pg_duckdb")
    pg.md_setup = False

    yield pg

    pg.cleanup()


@pytest.fixture
def pg(shared_pg, request):
    """
    Wraps the shared_pg fixture to reset the db after each test.

    It also creates a schema for the test to use. And logs the pg log to stdout
    for debugging failures.
    """
    shared_pg.reset()

    with shared_pg.log_path.open() as f:
        f.seek(0, os.SEEK_END)
        try:
            test_schema_name = request.node.name.removeprefix("test_")
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


@pytest.fixture
def md_cur(pg, ddb, request):
    """A cursor to a MotherDuck enabled pg_duckdb"""
    _ = ddb  # silence warning, we only need ddb
    test_db = request.node.name.removeprefix("test_")

    if not pg.md_setup:
        pg.sql("SELECT duckdb.enable_motherduck()")
        pg.md_setup = True

    pg.search_path = f"ddb${test_db}, public"
    with pg.cur() as cur:
        yield cur


@pytest.fixture
def ddb(request):
    """A DuckDB connection to MotherDuck

    This also creates a database for the test to use.
    """
    test_db = request.node.name.removeprefix("test_")
    ddb = duckdb.connect("md:")
    ddb.execute(f"DROP DATABASE IF EXISTS {test_db}")
    ddb.execute(f"CREATE DATABASE {test_db}")
    ddb.execute(f"USE {test_db}")

    try:
        yield Duckdb(ddb)
    finally:
        ddb.execute("USE my_db")
        ddb.execute(f"DROP DATABASE {test_db}")
