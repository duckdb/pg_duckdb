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
