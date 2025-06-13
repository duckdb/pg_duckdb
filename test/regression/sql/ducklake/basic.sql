CREATE TABLE t(a INT) USING ducklake;
INSERT INTO t SELECT g % 10 FROM generate_series(1, 1000) g;

SELECT COUNT(*) FROM t;
SELECT a, COUNT(*) FROM t WHERE a > 5 GROUP BY a ORDER BY a;


CREATE TABLE h(a INT);
INSERT INTO h SELECT g % 10 FROM generate_series(1, 1000) g;

-- insert into ducklake from an heap table
INSERT INTO t SELECT * FROM h;
SELECT COUNT(*) FROM t;

DROP TABLE t;
DROP TABLE h;

-- empty table
CREATE TABLE empty(a INT) USING ducklake;
SELECT COUNT(*) FROM empty;
DROP TABLE empty;
