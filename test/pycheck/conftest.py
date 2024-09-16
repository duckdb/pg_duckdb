import os
import pytest

from .utils import Postgres


@pytest.fixture(scope="session")
def shared_pg(tmp_path_factory):
    """Starts a new Postgres db that is shared for tests in this process"""
    pg = Postgres(tmp_path_factory.getbasetemp() / "pgdata")
    pg.initdb()

    pg.start()
    pg.sql("create extension pg_duckdb")

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
