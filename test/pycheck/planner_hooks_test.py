import pytest

from .utils import Postgres, require_extension


@pytest.mark.parametrize(
    "preload_libraries",
    ["pg_duckdb,pg_stat_statements", "pg_stat_statements,pg_duckdb"],
)
def test_postgres_scan_uses_planner_hooks(pg: Postgres, preload_libraries):
    """The scan of a Postgres table for DuckDB is planned through the planner hooks of other extensions (#1056)"""
    require_extension(pg, "pg_stat_statements")
    pg.configure(
        f"shared_preload_libraries = '{preload_libraries}'\n"
        # The scan query is planned while the DuckDB query runs, so it's nested
        "pg_stat_statements.track = 'all'\n"
        "pg_stat_statements.track_planning = on\n"
    )
    pg.restart()
    pg.sql("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")
    try:
        pg.sql("CREATE TABLE planner_hook_scan (a int)")
        pg.sql("INSERT INTO planner_hook_scan VALUES (1), (2), (3)")
        pg.sql("SELECT pg_stat_statements_reset()")
        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = true")
            assert cur.sql("SELECT max(a) FROM planner_hook_scan") == 3

        # The planner hook of pg_stat_statements counted the plans of the scan
        assert (
            pg.sql(
                "SELECT sum(plans) FROM pg_stat_statements WHERE query LIKE 'SELECT a FROM %.planner_hook_scan'"
            )
            == 1
        )
    finally:
        pg.sql("DROP TABLE IF EXISTS planner_hook_scan")
        pg.sql("DROP EXTENSION pg_stat_statements")
