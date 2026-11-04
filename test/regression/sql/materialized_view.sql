CREATE TABLE t(a INT, b VARCHAR);
INSERT INTO t SELECT g % 100, MD5(g::VARCHAR) FROM generate_series(1,1000) g;
SELECT COUNT(*) FROM t WHERE a % 10 = 0;
CREATE MATERIALIZED VIEW tv AS SELECT * FROM t WHERE a % 10 = 0;
SELECT COUNT(*) FROM tv;

INSERT INTO t SELECT g % 100, MD5(g::TEXT) FROM generate_series(1,1000) g;
SELECT COUNT(*) FROM t WHERE a % 10 = 0;

REFRESH MATERIALIZED VIEW tv;
SELECT COUNT(*) FROM tv;

SELECT COUNT(*) FROM t WHERE (a % 10 = 0) AND (a < 3);
SELECT COUNT(*) FROM tv WHERE a < 3;

DROP MATERIALIZED VIEW tv;
DROP TABLE t;

-- materialized view where duckdb execution changes result types of query
CREATE TABLE t_jsonb(data jsonb);
INSERT INTO t_jsonb VALUES ('{"a": 1, "b": 2}');
CREATE MATERIALIZED VIEW mv_json AS SELECT * FROM t_jsonb;
SELECT * from mv_json;
-- Should return json, because that's the return type of the query duckdb query
-- (since it does not have the jsonb type).
SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'mv_json'::regclass AND attname = 'data';
INSERT INTO t_jsonb VALUES ('{"a": 3, "b": 4}');
REFRESH MATERIALIZED VIEW mv_json;
SELECT * from mv_json;
SET duckdb.force_execution = false;
INSERT INTO t_jsonb VALUES ('{"a": 5, "b": 6}');
REFRESH MATERIALIZED VIEW mv_json;
SET duckdb.force_execution = true;

DROP MATERIALIZED VIEW mv_json;

-- Materialized view created without duckdb execution, and then refresh with
-- duckdb execution enabled.
SET duckdb.force_execution = false;
CREATE MATERIALIZED VIEW mv_jsonb AS SELECT * FROM t_jsonb;
SELECT * from mv_jsonb;
SELECT atttypid::regtype FROM pg_attribute WHERE attrelid = 'mv_jsonb'::regclass AND attname = 'data';
REFRESH MATERIALIZED VIEW mv_jsonb;
SELECT * from mv_jsonb;
SET duckdb.force_execution = true;
REFRESH MATERIALIZED VIEW mv_jsonb;
SET duckdb.force_execution = false;
SELECT * from mv_jsonb;
SET duckdb.force_execution = true;
DROP MATERIALIZED VIEW mv_jsonb;

-- materialized view from duckdb execution

CREATE TABLE t_csv(a INT, b INT);
INSERT INTO t_csv VALUES (1,1),(2,2),(3,3);

\set pwd `pwd`
\set csv_file_path '\'' :pwd '/tmp_check/t_csv.csv'  '\''

COPY t_csv TO :csv_file_path (FORMAT CSV, HEADER TRUE, DELIMITER ',');

CREATE MATERIALIZED VIEW mv_csv AS SELECT * FROM read_csv(:csv_file_path);

SELECT COUNT(*) FROM mv_csv;
SELECT * FROM mv_csv;

INSERT INTO t_csv VALUES (4,4);
COPY t_csv TO :csv_file_path (FORMAT CSV, HEADER TRUE, DELIMITER ',');
REFRESH MATERIALIZED VIEW mv_csv;

SELECT COUNT(*) FROM mv_csv;
SELECT * FROM mv_csv;

DROP MATERIALIZED VIEW mv_csv;
DROP TABLE t_csv;

-- Materialized view over NUMERIC columns without a precision. DuckDB reads
-- those as DOUBLE, and the materialized view columns must get that type too.
-- Otherwise float8 datums end up in numeric columns, which corrupts the heap
-- and crashes later reads such as count(fees) or ANALYZE.
-- See https://github.com/duckdb/pg_duckdb/issues/1059
CREATE TABLE t_numeric(id int, grp text, amount numeric, fees numeric, currency text);
INSERT INTO t_numeric
SELECT g, 'grp-' || (g % 50), (g % 10000)::numeric / 100, (g % 100)::numeric / 100,
    CASE WHEN g % 2 = 0 THEN 'USD' ELSE 'EUR' END
FROM generate_series(1, 5000) g;

-- Without the GUC DuckDB refuses NUMERICs without a precision
CREATE MATERIALIZED VIEW mv_numeric_unsupported AS
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC;

SET duckdb.convert_unsupported_numeric_to_double = true;
-- DISTINCT ON is executed by DuckDB using arg_min/arg_max
CREATE MATERIALIZED VIEW mv_numeric AS
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC;
SELECT attname, atttypid::regtype FROM pg_attribute
WHERE attrelid = 'mv_numeric'::regclass AND attnum > 0 ORDER BY attnum;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM mv_numeric;
SELECT * FROM mv_numeric ORDER BY grp LIMIT 3;
ANALYZE mv_numeric;
SELECT attname, null_frac, n_distinct FROM pg_stats
WHERE schemaname = 'public' AND tablename = 'mv_numeric' AND attname IN ('amount', 'fees') ORDER BY attname;

INSERT INTO t_numeric VALUES (6000, 'grp-0', 123.45, 0.5, 'USD');
REFRESH MATERIALIZED VIEW mv_numeric;
SELECT * FROM mv_numeric WHERE grp = 'grp-0';
SET duckdb.force_execution = false;
REFRESH MATERIALIZED VIEW mv_numeric;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM mv_numeric;
SET duckdb.force_execution = true;
ANALYZE mv_numeric;

-- WITH NO DATA defines the columns without planning the query, and the
-- REFRESH then fills them. This is also how CREATE MATERIALIZED VIEW loads
-- its data since PG17.
CREATE MATERIALIZED VIEW mv_numeric_no_data AS
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC
WITH NO DATA;
SELECT attname, atttypid::regtype FROM pg_attribute
WHERE attrelid = 'mv_numeric_no_data'::regclass AND attname IN ('amount', 'fees') ORDER BY attnum;
REFRESH MATERIALIZED VIEW mv_numeric_no_data;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM mv_numeric_no_data;
ANALYZE mv_numeric_no_data;

-- Materialized view created with Postgres execution, so it has numeric
-- columns. A REFRESH must keep using Postgres execution, even when
-- duckdb.force_execution is enabled.
SET duckdb.force_execution = false;
CREATE MATERIALIZED VIEW mv_numeric_pg AS
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC;
SET duckdb.force_execution = true;
SELECT attname, atttypid::regtype FROM pg_attribute
WHERE attrelid = 'mv_numeric_pg'::regclass AND attname IN ('amount', 'fees') ORDER BY attnum;
REFRESH MATERIALIZED VIEW mv_numeric_pg;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM mv_numeric_pg;
ANALYZE mv_numeric_pg;

-- Same for CREATE TABLE AS
CREATE TABLE ctas_numeric AS
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC;
SELECT attname, atttypid::regtype FROM pg_attribute
WHERE attrelid = 'ctas_numeric'::regclass AND attnum > 0 ORDER BY attnum;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM ctas_numeric;
ANALYZE ctas_numeric;

-- And INSERT ... SELECT into a numeric column
CREATE TABLE insert_numeric(grp text, id int, amount numeric, fees numeric, currency text);
INSERT INTO insert_numeric
SELECT DISTINCT ON (grp) grp, id, amount, fees, currency FROM t_numeric ORDER BY grp, id DESC;
SELECT count(*), count(amount), count(fees), count(currency), round(sum(amount)::numeric, 2), round(sum(fees)::numeric, 2) FROM insert_numeric;
ANALYZE insert_numeric;

RESET duckdb.convert_unsupported_numeric_to_double;
DROP TABLE insert_numeric;
DROP TABLE ctas_numeric;
DROP MATERIALIZED VIEW mv_numeric_pg;
DROP MATERIALIZED VIEW mv_numeric_no_data;
DROP MATERIALIZED VIEW mv_numeric;
DROP MATERIALIZED VIEW IF EXISTS mv_numeric_unsupported;
DROP TABLE t_numeric;
