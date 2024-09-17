from .utils import Cursor


def test_prepared(cur: Cursor):
    cur.sql("CREATE TABLE test_table (id int)")

    q1 = "SELECT count(*) FROM test_table"
    # Run it 6 times because after the 5th time some special logic kicks in.
    # See this link for more details:
    # https://github.com/postgres/postgres/blob/89f908a6d0ac1337c868625008c9598487d184e7/src/backend/utils/cache/plancache.c#L1073-L1075
    assert cur.sql(q1, prepare=True) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0
    assert cur.sql(q1) == 0

    cur.sql("INSERT INTO test_table VALUES (1), (2), (3)")
    # Ensure that the result is different now
    assert cur.sql(q1) == 3

    q2 = "SELECT count(*) FROM test_table where id = %s"
    assert cur.sql(q2, (1,), prepare=True) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (1,)) == 1
    assert cur.sql(q2, (1,)) == 1

    assert cur.sql(q2, (4,)) == 0
