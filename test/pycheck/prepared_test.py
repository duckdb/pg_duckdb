from .utils import Cursor


def test_prepared(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int)")

    q1 = "SELECT count(*) FROM test_table"
    # Run it 6 times because after the 5th time some special logic kicks in
    assert cur.sql(q1, prepare=True) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
