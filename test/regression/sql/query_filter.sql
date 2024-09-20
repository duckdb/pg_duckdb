CREATE TABLE query_filter_int(a INT);
INSERT INTO query_filter_int SELECT g FROM generate_series(1,100) g;
SELECT COUNT(*) FROM query_filter_int WHERE a  <= 50;
DROP TABLE query_filter_int;

CREATE TABLE query_filter_float(a FLOAT8);
INSERT INTO query_filter_float VALUES (0.9), (1.0), (1.1);
SELECT COUNT(*) FROM query_filter_float WHERE a < 1.0;
SELECT COUNT(*) FROM query_filter_float WHERE a <= 1.0;
SELECT COUNT(*) FROM query_filter_float WHERE a < 1.1;

DROP TABLE query_filter_float;
