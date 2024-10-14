from .utils import Cursor

import pytest
import psycopg.errors


def test_explain(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int, name text)")
    result = cur.sql("EXPLAIN SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" not in plan

    result = cur.sql("EXPLAIN ANALYZE SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "Query Profiling Information" in plan
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" in plan

    result = cur.sql("EXPLAIN SELECT count(*) FROM test_table where id = %s", (1,))
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "id=1 AND id IS NOT NULL" in plan
    assert "Total Time:" not in plan

    result = cur.sql(
        "EXPLAIN ANALYZE SELECT count(*) FROM test_table where id = %s", (1,)
    )
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "id=1 AND id IS NOT NULL" in plan
    assert "Total Time:" in plan


def test_explain_ctas(cur: Cursor):
    cur.sql("CREATE TEMP TABLE heap1(id) AS SELECT 1")
    result = cur.sql("EXPLAIN CREATE TEMP TABLE heap2(id) AS SELECT * from heap1")
    plan = "\n".join(result)
    assert "POSTGRES_SEQ_SCAN" in plan
    assert "Total Time:" not in plan

    result = cur.sql(
        "EXPLAIN ANALYZE CREATE TEMP TABLE heap2(id) AS SELECT * from heap1"
    )
    plan = "\n".join(result)
    assert "POSTGRES_SEQ_SCAN" in plan
    assert "Total Time:" in plan

    result = cur.sql(
        "EXPLAIN CREATE TEMP TABLE duckdb1(id) USING duckdb AS SELECT * from heap1"
    )
    plan = "\n".join(result)
    assert "POSTGRES_SEQ_SCAN" in plan
    assert "Total Time:" not in plan

    # EXPLAIN ANALYZE is not supported for DuckDB CTAS (yet)
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        cur.sql(
            "EXPLAIN ANALYZE CREATE TEMP TABLE duckdb2(id) USING duckdb AS SELECT * from heap1"
        )
