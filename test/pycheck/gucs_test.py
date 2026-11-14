import pytest

from .utils import Cursor, Postgres


@pytest.mark.parametrize(
    "name", ["duckdb.temporary_directory", "duckdb.extension_directory"]
)
def test_directory_guc_reset_on_reload(pg: Postgres, cur: Cursor, name: str):
    """Removing the GUC from the config used to reset it to garbage in running backends (#1005)"""
    default = cur.sql(f"SHOW {name}")

    pg.configure(f"{name} = '/tmp/pg_duckdb_custom_dir'")
    pg.reload()
    assert cur.sql(f"SHOW {name}") == "/tmp/pg_duckdb_custom_dir"

    # Without the setting in the config, the running backend resets it to its
    # built-in default
    pg.configure("")
    pg.reload()
    assert cur.sql(f"SHOW {name}") == default
