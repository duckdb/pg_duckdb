"""
Tests for using pg_duckdb together with other extensions. Each test skips
itself if its extension is not installed, see require_extension. The
"Other extensions" CI workflow installs them.
"""

from decimal import Decimal

import psycopg.errors
import pytest

from .utils import Postgres, require_extension


@pytest.fixture(params=["timescaledb,pg_duckdb", "pg_duckdb,timescaledb"])
def timescaledb(pg: Postgres, request):
    """TimescaleDB, loaded before and after pg_duckdb"""
    require_extension(pg, "timescaledb")
    pg.configure(
        f"shared_preload_libraries = '{request.param}'\n"
        "timescaledb.telemetry_level = off\n"
    )
    pg.restart()
    # In its own schema, because pg_duckdb has a time_bucket function in public
    pg.sql("CREATE SCHEMA IF NOT EXISTS timescaledb")
    pg.sql("CREATE EXTENSION IF NOT EXISTS timescaledb SCHEMA timescaledb")
    try:
        yield pg
    finally:
        pg.sql("DROP EXTENSION timescaledb")
        pg.sql("DROP SCHEMA timescaledb")


def test_timescaledb(timescaledb: Postgres, tmp_path):
    """Scanning Postgres tables for DuckDB used to crash with TimescaleDB loaded (#1056)"""
    pg = timescaledb
    try:
        pg.sql("CREATE TABLE heap_table (id int, label text)")
        pg.sql(
            "INSERT INTO heap_table SELECT i, 'label' || i FROM generate_series(1, 100) i"
        )
        pg.sql("CREATE TABLE hypertable (time timestamptz NOT NULL, value int)")
        pg.sql("SELECT timescaledb.create_hypertable('hypertable', 'time')")
        pg.sql(
            "INSERT INTO hypertable SELECT now() - i * interval '1 day', i FROM generate_series(1, 100) i"
        )

        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = true")
            assert cur.sql("SELECT count(*) FROM heap_table") == 100
            assert cur.sql("SELECT label FROM heap_table WHERE id = 42") == "label42"
            assert cur.sql("SELECT count(*), sum(value) FROM hypertable") == (100, 5050)

            # TimescaleDB plans INSERTs into hypertables with its own node on
            # top, so those can't run in DuckDB and fall back to Postgres.
            cur.sql("INSERT INTO heap_table SELECT id + 100, label FROM heap_table")
            cur.sql(
                "INSERT INTO hypertable SELECT time - interval '1 year', value FROM hypertable"
            )
            assert cur.sql("SELECT count(*) FROM heap_table") == 200
            assert cur.sql("SELECT count(*) FROM hypertable") == 200
            with pytest.raises(
                psycopg.errors.InternalError,
                match="DuckDB does not support INSERT into this table",
            ):
                cur.sql(
                    "INSERT INTO hypertable SELECT now(), r['value']::int FROM duckdb.query($$ SELECT 1 AS value $$) r"
                )

        parquet_path = tmp_path / "heap_table.parquet"
        pg.sql(f"COPY heap_table TO '{parquet_path}' (FORMAT parquet)")
        assert pg.sql(f"SELECT count(*) FROM read_parquet('{parquet_path}')") == 200
    finally:
        pg.sql("DROP TABLE IF EXISTS heap_table, hypertable")


def test_timescaledb_with_duckdb_queries(timescaledb: Postgres, tmp_path):
    """Joining hypertables with Parquet files and duckdb.query() results"""
    pg = timescaledb
    try:
        pg.sql(
            "CREATE TABLE metrics (time timestamptz NOT NULL, device_id int, value int)"
        )
        pg.sql("SELECT timescaledb.create_hypertable('metrics', 'time')")
        pg.sql(
            "INSERT INTO metrics SELECT '2026-01-01 00:00:00+00'::timestamptz + i * interval '1 hour', i % 4, i"
            " FROM generate_series(1, 200) i"
        )
        devices = tmp_path / "devices.parquet"
        pg.sql(
            "COPY (SELECT i AS id, 'device' || i AS name FROM generate_series(0, 3) i)"
            f" TO '{devices}' (FORMAT parquet)"
        )

        with pg.cur() as cur:
            # These queries run in DuckDB, which scans the hypertable
            cur.sql("SET duckdb.force_execution = true")
            assert cur.sql(
                f"SELECT d['name'], count(*), sum(m.value) FROM read_parquet('{devices}') d"
                " JOIN metrics m ON m.device_id = d['id']::int"
                " WHERE m.time >= '2026-01-05' GROUP BY 1 ORDER BY 1"
            ) == [
                ("device0", 27, 3996),
                ("device1", 26, 3822),
                ("device2", 26, 3848),
                ("device3", 26, 3874),
            ]
            assert cur.sql(
                "SELECT count(*), sum(value) FROM metrics WHERE device_id IN"
                " (SELECT r['id']::int FROM duckdb.query($$ SELECT 1 AS id UNION ALL SELECT 2 $$) r)"
            ) == (100, 9950)
            assert cur.sql(
                "SELECT r['label'], s.total FROM (SELECT device_id, sum(value) AS total FROM metrics GROUP BY device_id) s"
                " JOIN duckdb.query($$ SELECT 3 AS id, 'three' AS label $$) r ON r['id']::int = s.device_id"
            ) == ("three", 5050)

            # The time_bucket function of DuckDB gives the same buckets as the
            # one of TimescaleDB
            duckdb_buckets = cur.sql(
                "SELECT time_bucket(interval '1 day', m.time AT TIME ZONE 'UTC'), count(*)"
                f" FROM metrics m JOIN read_parquet('{devices}') d ON m.device_id = d['id']::int"
                " GROUP BY 1 ORDER BY 1"
            )
            cur.sql("SET duckdb.force_execution = false")
            assert duckdb_buckets == cur.sql(
                "SELECT timescaledb.time_bucket(interval '1 day', time AT TIME ZONE 'UTC'), count(*)"
                " FROM metrics GROUP BY 1 ORDER BY 1"
            )

            # The functions of TimescaleDB don't exist in DuckDB
            for function in [
                "timescaledb.time_bucket(interval '1 day', m.time)",
                "timescaledb.first(m.value, m.time)",
            ]:
                with pytest.raises(psycopg.errors.InternalError, match="Binder Error"):
                    cur.sql(
                        f"SELECT {function} FROM metrics m"
                        f" JOIN read_parquet('{devices}') d ON m.device_id = d['id']::int"
                    )
    finally:
        pg.sql("DROP TABLE IF EXISTS metrics")


@pytest.fixture
def citus(pg: Postgres):
    """A single node Citus cluster, where the coordinator also stores the shards"""
    require_extension(pg, "citus")
    pg.configure(
        # Citus has to be loaded first
        "shared_preload_libraries = 'citus,pg_duckdb'\n"
        # Only use DuckDB in the test sessions, not for the extension script
        # or the internal connections of Citus, which DuckDB can't handle yet,
        # e.g. it doesn't support the subtransactions that Citus uses when
        # dropping shards.
        "duckdb.force_execution = false\n"
    )
    pg.restart()
    pg.sql("CREATE EXTENSION IF NOT EXISTS citus")
    try:
        pg.sql(f"SELECT citus_set_coordinator_host('{pg.host}', {pg.port})")
        pg.sql(
            f"SELECT citus_set_node_property('{pg.host}', {pg.port}, 'shouldhaveshards', true)"
        )
        yield pg
    finally:
        pg.sql("DROP EXTENSION citus")


def assert_same_result_as_citus(pg: Postgres, query: str):
    """Runs the query with DuckDB and with Postgres, i.e. Citus, and compares the results"""
    with pg.cur() as cur:
        cur.sql("SET duckdb.force_execution = true")
        duckdb_result = cur.sql(query)
        cur.sql("SET duckdb.force_execution = false")
        assert duckdb_result == cur.sql(query), query


def test_citus_tables(citus: Postgres, tmp_path):
    """Scanning Postgres tables for DuckDB used to fail an assertion in Citus (#1056)"""
    pg = citus
    try:
        pg.sql("CREATE TABLE local_table (id bigint, log text)")
        pg.sql("INSERT INTO local_table VALUES (1, 'test')")
        pg.sql("CREATE TABLE dist_table (id bigint, value int)")
        pg.sql("SELECT create_distributed_table('dist_table', 'id')")
        pg.sql("INSERT INTO dist_table SELECT i, i FROM generate_series(1, 100) i")
        pg.sql("CREATE TABLE ref_table (id bigint, name text)")
        pg.sql("SELECT create_reference_table('ref_table')")
        pg.sql(
            "INSERT INTO ref_table SELECT i, 'name' || i FROM generate_series(1, 10) i"
        )

        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = true")
            assert cur.sql("SELECT * FROM local_table") == (1, "test")
            assert cur.sql("SELECT count(*) FROM local_table") == 1
            assert cur.sql("SELECT count(*) FROM dist_table") == 100
            assert cur.sql("SELECT sum(value) FROM dist_table WHERE id <= 10") == 55
            assert cur.sql("SELECT name FROM ref_table WHERE id = 3") == "name3"
            assert (
                cur.sql("SELECT count(*) FROM dist_table JOIN ref_table USING (id)")
                == 10
            )

            # Citus plans INSERTs into distributed tables with its own node, so
            # those fall back to Postgres
            cur.sql("INSERT INTO dist_table SELECT id + 100, value FROM dist_table")
            assert cur.sql("SELECT count(*) FROM dist_table") == 200

        parquet_path = tmp_path / "dist_table.parquet"
        pg.sql(f"COPY dist_table TO '{parquet_path}' (FORMAT parquet)")
        assert pg.sql(f"SELECT count(*) FROM read_parquet('{parquet_path}')") == 200
    finally:
        pg.sql("DROP TABLE IF EXISTS local_table, dist_table, ref_table")


def test_citus_complex_queries(citus: Postgres):
    """Queries that Citus plans with subplans or intermediate results

    DuckDB only asks Citus to scan the tables and runs the rest of the query
    itself, so it must get the same results as Citus.
    """
    pg = citus
    try:
        pg.sql("CREATE TABLE orders (id bigint, customer_id bigint, amount int)")
        pg.sql("CREATE TABLE customers (id bigint, region text)")
        pg.sql("SELECT create_distributed_table('orders', 'customer_id')")
        pg.sql("SELECT create_distributed_table('customers', 'id')")
        pg.sql(
            "INSERT INTO orders SELECT i, i % 50, i % 97 FROM generate_series(1, 2000) i"
        )
        pg.sql(
            "INSERT INTO customers SELECT i, 'region' || (i % 4) FROM generate_series(0, 49) i"
        )

        # Citus runs the CTE with LIMIT as a subplan and sends its result to
        # the nodes with COPY, which pg_duckdb used to intercept.
        assert_same_result_as_citus(
            pg,
            "WITH top AS (SELECT customer_id, amount FROM orders ORDER BY amount DESC, id LIMIT 20)"
            " SELECT c.region, count(*), sum(t.amount) FROM top t JOIN customers c ON c.id = t.customer_id"
            " GROUP BY c.region ORDER BY c.region",
        )
        # A join that is not on the distribution columns, which Citus plans
        # recursively with intermediate results
        assert_same_result_as_citus(
            pg,
            "SELECT count(*) FROM (SELECT amount FROM orders) o"
            " JOIN (SELECT id FROM customers OFFSET 0) c ON o.amount = c.id",
        )
        # A co-located join with aggregates and a window function
        assert_same_result_as_citus(
            pg,
            "SELECT region, total, rank() OVER (ORDER BY total DESC) FROM ("
            " SELECT c.region, sum(o.amount) AS total FROM orders o JOIN customers c ON c.id = o.customer_id"
            " GROUP BY c.region) s ORDER BY region",
        )
        # A correlated subquery
        assert_same_result_as_citus(
            pg,
            "SELECT count(*) FROM customers c"
            " WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id AND o.amount > 90)",
        )
        assert_same_result_as_citus(
            pg, "SELECT count(DISTINCT amount), avg(amount)::numeric(10, 4) FROM orders"
        )
    finally:
        pg.sql("DROP TABLE IF EXISTS orders, customers")


def test_citus_columnar(citus: Postgres, tmp_path):
    """Citus columnar tables, which Citus scans with its own node"""
    pg = citus
    require_extension(pg, "citus_columnar")
    pg.sql("CREATE EXTENSION IF NOT EXISTS citus_columnar")
    try:
        pg.sql("CREATE TABLE columnar_table (id bigint, value int) USING columnar")
        pg.sql(
            "INSERT INTO columnar_table SELECT i, i % 10 FROM generate_series(1, 1000) i"
        )
        pg.sql("CREATE TABLE dist_columnar (id bigint, value int) USING columnar")
        pg.sql("SELECT create_distributed_table('dist_columnar', 'id')")
        pg.sql(
            "INSERT INTO dist_columnar SELECT i, i % 10 FROM generate_series(1, 1000) i"
        )

        assert_same_result_as_citus(
            pg, "SELECT id, value FROM columnar_table WHERE id <= 3 ORDER BY id"
        )
        assert_same_result_as_citus(pg, "SELECT count(*) FROM columnar_table")
        assert_same_result_as_citus(
            pg,
            "SELECT value, count(*) FROM columnar_table WHERE value > 5 GROUP BY value ORDER BY value",
        )
        assert_same_result_as_citus(
            pg, "SELECT count(*), sum(value) FROM dist_columnar"
        )
        assert_same_result_as_citus(
            pg,
            "SELECT count(*) FROM columnar_table c JOIN dist_columnar d USING (id)",
        )

        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = true")
            # Columnar tables are planned as a plain INSERT, so the source of
            # the INSERT runs in DuckDB
            cur.sql(
                "INSERT INTO columnar_table SELECT id + 1000, value FROM columnar_table"
            )
            cur.sql(
                "INSERT INTO columnar_table SELECT r['id']::bigint, r['value']::int"
                " FROM duckdb.query($$ SELECT 5000 AS id, 1 AS value $$) r"
            )
            assert cur.sql("SELECT count(*) FROM columnar_table") == 2001

        parquet_path = tmp_path / "columnar_table.parquet"
        pg.sql(f"COPY columnar_table TO '{parquet_path}' (FORMAT parquet)")
        assert pg.sql(f"SELECT count(*) FROM read_parquet('{parquet_path}')") == 2001
    finally:
        pg.sql("DROP TABLE IF EXISTS columnar_table, dist_columnar")
        pg.sql("DROP EXTENSION IF EXISTS citus_columnar")


def test_pgvector(pg: Postgres, tmp_path):
    """DuckDB doesn't support pgvector types, so queries on them fall back to Postgres"""
    require_extension(pg, "vector")
    pg.sql("CREATE EXTENSION IF NOT EXISTS vector")
    try:
        pg.sql(
            "CREATE TABLE items (id int, embedding vector(3), half halfvec(3), tag text)"
        )
        pg.sql(
            "INSERT INTO items VALUES (1, '[1,2,3]', '[1,2,3]', 'a'), (2, '[4,5,6]', '[4,5,6]', 'b'), (3, '[1,1,1]', '[1,1,1]', 'c')"
        )
        pg.sql("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)")

        with pg.cur() as cur:
            cur.sql("SET duckdb.force_execution = true")
            # Columns of other types are read by DuckDB
            assert cur.sql("SELECT id, tag FROM items ORDER BY id") == [
                (1, "a"),
                (2, "b"),
                (3, "c"),
            ]
            assert (
                cur.sql("SELECT count(*) FROM items WHERE embedding IS NOT NULL") == 3
            )
            # Queries on pgvector types and operators fall back to Postgres
            assert cur.sql("SELECT embedding, half FROM items WHERE id = 2") == (
                "[4,5,6]",
                "[4,5,6]",
            )
            assert cur.sql(
                "SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 2"
            ) == [1, 3]

        # DuckDB can't write pgvector types, but the other columns work
        with pytest.raises(
            psycopg.errors.InternalError, match="Unsupported PostgreSQL type"
        ):
            pg.sql(f"COPY items TO '{tmp_path / 'items.parquet'}' (FORMAT parquet)")
        parquet_path = tmp_path / "items_tags.parquet"
        pg.sql(f"COPY (SELECT id, tag FROM items) TO '{parquet_path}' (FORMAT parquet)")
        assert pg.sql(f"SELECT count(*) FROM read_parquet('{parquet_path}')") == 3
    finally:
        pg.sql("DROP TABLE IF EXISTS items")
        pg.sql("DROP EXTENSION vector")


# Queries with duckdb.query() run completely in DuckDB, which has no pgvector
# types or operators. So combining it with pgvector columns in one query
# fails, but it must fail with an error and not crash.
PGVECTOR_WITH_DUCKDB_QUERY_ERRORS = [
    # Join that reads a vector distance
    (
        "SELECT i.id, i.embedding <-> '[1,2,3]' FROM items i"
        " JOIN duckdb.query($$ SELECT 1 AS id UNION ALL SELECT 3 $$) r ON r['id']::int = i.id",
        "Type with name vector does not exist",
    ),
    # duckdb.query() as IN subquery of a nearest neighbour query
    (
        "SELECT id FROM items WHERE id IN (SELECT r['id']::int FROM duckdb.query($$ SELECT 1 AS id $$) r)"
        " ORDER BY embedding <-> '[1,2,3]' LIMIT 2",
        "Type with name vector does not exist",
    ),
    # Nearest neighbour subquery joined with duckdb.query()
    (
        "SELECT nn.id, r['label'] FROM (SELECT id FROM items ORDER BY embedding <-> '[1,2,3]' LIMIT 2) nn"
        " JOIN duckdb.query($$ SELECT 1 AS id, 'one' AS label $$) r ON r['id']::int = nn.id",
        "Type with name vector does not exist",
    ),
    # duckdb.query() as scalar subquery of a nearest neighbour query
    (
        "SELECT id, (SELECT r['x']::int FROM duckdb.query($$ SELECT 42 AS x $$) r) FROM items"
        " ORDER BY embedding <-> '[1,2,3]' LIMIT 1",
        "Type with name vector does not exist",
    ),
    # duckdb.query() in a CTE joined with the vector column
    (
        "WITH d AS (SELECT r['id']::int AS id FROM duckdb.query($$ SELECT 3 AS id $$) r)"
        " SELECT i.id, i.embedding FROM items i JOIN d USING (id)",
        "Unsupported PostgreSQL type",
    ),
]


def test_pgvector_with_duckdb_query(pg: Postgres):
    """Combining duckdb.query() with pgvector queries"""
    require_extension(pg, "vector")
    pg.sql("CREATE EXTENSION IF NOT EXISTS vector")
    try:
        pg.sql("CREATE TABLE items (id int, embedding vector(3), tag text)")
        pg.sql(
            "INSERT INTO items VALUES (1, '[1,2,3]', 'a'), (2, '[4,5,6]', 'b'), (3, '[1,1,1]', 'c')"
        )
        pg.sql("CREATE INDEX ON items USING hnsw (embedding vector_l2_ops)")

        with pg.cur() as cur:
            # Joining duckdb.query() with the other columns of the table works
            assert (
                cur.sql(
                    "SELECT i.tag FROM items i JOIN duckdb.query($$ SELECT 2 AS id $$) r ON r['id']::int = i.id"
                )
                == "b"
            )

            for query, error in PGVECTOR_WITH_DUCKDB_QUERY_ERRORS:
                with pytest.raises(psycopg.errors.InternalError, match=error):
                    cur.sql(query)

            # DuckDB can still be used afterwards
            assert cur.sql("SELECT * FROM duckdb.query($$ SELECT 42 AS x $$)") == 42

            # To combine them, store the result of duckdb.query() in a table
            # first, and join it in a pgvector query that runs in Postgres
            cur.sql(
                "CREATE TEMP TABLE scores AS SELECT r['id']::int AS id, r['score']::float8 AS score"
                " FROM duckdb.query($$ SELECT 1 AS id, 0.5 AS score UNION ALL SELECT 3, 0.9 $$) r"
            )
            cur.sql("SET duckdb.force_execution = false")
            assert cur.sql(
                "SELECT s.id, s.score, round((i.embedding <-> '[1,2,3]')::numeric, 3)"
                " FROM scores s JOIN items i USING (id) ORDER BY 3"
            ) == [(1, 0.5, Decimal("0.000")), (3, 0.9, Decimal("2.236"))]
    finally:
        pg.sql("DROP TABLE IF EXISTS items")
        pg.sql("DROP EXTENSION vector")
