CREATE TABLE tbl (id int, c float8, d text);
INSERT INTO tbl SELECT i, i * 2.0, 'helloworld' FROM generate_series(1, 1000000) i;
SELECT * FROM tbl ORDER BY 1,2,3 LIMIT 3;
DROP TABLE tbl;

-- JSON/LIST type
CREATE TABLE tbl (id int, c jsonb, d text[]);
INSERT INTO tbl SELECT i, jsonb_build_object('a', i), array_agg(i) FROM generate_series(1, 500000) i GROUP BY i;
SELECT * FROM tbl ORDER BY id DESC LIMIT 3;
DROP TABLE tbl;
