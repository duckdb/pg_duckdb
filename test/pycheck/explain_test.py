"""Tests for EXPLAIN

These tests are using Python mainly because the output of EXPLAIN ANALYZE
contains timings, so the output is not deterministic.
"""

from .utils import Cursor

import pytest
import psycopg.errors
import json


def test_explain(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int, name text)")
    result = cur.sql("EXPLAIN SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" not in plan
    assert "Output:" not in plan

    result = cur.sql("EXPLAIN ANALYZE SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "Query Profiling Information" in plan
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" in plan
    assert "Output:" not in plan

    result = cur.sql("EXPLAIN SELECT count(*) FROM test_table where id = %s", (1,))
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "id=1" in plan
    assert "Total Time:" not in plan
    assert "Output:" not in plan

    result = cur.sql(
        "EXPLAIN ANALYZE SELECT count(*) FROM test_table where id = %s", (1,)
    )
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "id=1" in plan
    assert "Total Time:" in plan
    assert "Output:" not in plan

    result = cur.sql("EXPLAIN VERBOSE SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" not in plan
    assert "Output:" in plan

    result = cur.sql("EXPLAIN (VERBOSE, ANALYZE) SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "Query Profiling Information" in plan
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" in plan
    assert "Output:" in plan

    # Test for Json Output Format , psycopg internal convert json to dict
    result = cur.sql("EXPLAIN (FORMAT JSON) SELECT count(*) FROM test_table")
    assert len(result) == 1
    assert type(result[0]) == dict
    plan=json.dumps(result[0])
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time" not in plan
    assert "Output" not in plan

    result = cur.sql("EXPLAIN (VERBOSE, FORMAT JSON) SELECT count(*) FROM test_table")
    assert len(result) == 1
    assert type(result[0]) == dict
    plan=json.dumps(result[0])
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time" not in plan
    assert "Output" in plan

    result = cur.sql("EXPLAIN (VERBOSE, ANALYZE, FORMAT JSON) SELECT count(*) FROM test_table")
    assert len(result) == 1
    assert type(result[0]) == dict
    plan=json.dumps(result[0])
    assert "EXPLAIN_ANALYZE" in plan
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time" in plan
    assert "Output" in plan


def test_explain_ctas(cur: Cursor):
    cur.sql("CREATE TEMP TABLE heap1(id) AS SELECT 1")
    result = cur.sql("EXPLAIN CREATE TEMP TABLE heap2(id) AS SELECT * from heap1")
    plan = "\n".join(result)
    assert "POSTGRES_SCAN" in plan
    assert "Total Time:" not in plan

    result = cur.sql(
        "EXPLAIN ANALYZE CREATE TEMP TABLE heap2(id) AS SELECT * from heap1"
    )
    plan = "\n".join(result)
    assert "TABLE_SCAN" in plan
    assert "Total Time:" in plan

    result = cur.sql(
        "EXPLAIN CREATE TEMP TABLE duckdb1(id) USING duckdb AS SELECT * from heap1"
    )
    plan = "\n".join(result)
    assert "POSTGRES_SCAN" in plan
    assert "Total Time:" not in plan

    # EXPLAIN ANALYZE is not supported for DuckDB CTAS (yet)
    with pytest.raises(psycopg.errors.FeatureNotSupported):
        cur.sql(
            "EXPLAIN ANALYZE CREATE TEMP TABLE duckdb2(id) USING duckdb AS SELECT * from heap1"
        )
