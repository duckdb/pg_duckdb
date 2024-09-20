from .utils import Cursor


def test_explain(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int primary key, name text)")
    result = cur.sql("EXPLAIN SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" not in plan

    result = cur.sql("EXPLAIN ANALYZE SELECT count(*) FROM test_table")
    plan = "\n".join(result)
    assert "Query Profiling Information" in plan
    assert "UNGROUPED_AGGREGATE" in plan
    assert "Total Time:" in plan
