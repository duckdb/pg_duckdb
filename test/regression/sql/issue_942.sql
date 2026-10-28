-- a float4 column compared to a literal: the pushed-down filter must keep
-- the float4 semantics Postgres applies, so both engines agree
CREATE TABLE float4_tbl (f1 float4);
INSERT INTO float4_tbl VALUES ('1004.30   '), ('0.1'), ('-2.5');
CREATE TABLE float8_tbl (f1 float8);
INSERT INTO float8_tbl VALUES ('1004.30   ');

SET duckdb.force_execution = off;
SELECT * FROM float4_tbl WHERE f1 = '1004.3';
SELECT * FROM float4_tbl WHERE f1 = 1004.3;
SELECT * FROM float4_tbl WHERE f1 = '0.1';
SELECT * FROM float4_tbl WHERE f1 > '0.1' ORDER BY f1;
SELECT * FROM float4_tbl WHERE f1 <= '0.1' ORDER BY f1;
SELECT * FROM float4_tbl WHERE f1 <> '1004.3' ORDER BY f1;
SELECT * FROM float8_tbl WHERE f1 = 1004.3;

SET duckdb.force_execution = on;
SELECT * FROM float4_tbl WHERE f1 = '1004.3';
SELECT * FROM float4_tbl WHERE f1 = 1004.3;
SELECT * FROM float4_tbl WHERE f1 = '0.1';
SELECT * FROM float4_tbl WHERE f1 > '0.1' ORDER BY f1;
SELECT * FROM float4_tbl WHERE f1 <= '0.1' ORDER BY f1;
SELECT * FROM float4_tbl WHERE f1 <> '1004.3' ORDER BY f1;
SELECT * FROM float8_tbl WHERE f1 = 1004.3;

DROP TABLE float4_tbl;
DROP TABLE float8_tbl;
