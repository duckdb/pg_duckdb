-- INT4 (zero dimension)
CREATE TABLE int_array_0d(a INT[]);
INSERT INTO int_array_0d SELECT CAST(a as INT[]) FROM (VALUES
    ('{}')
) t(a);
SELECT * FROM int_array_0d;

-- INT4 (single dimension)
CREATE TABLE int_array_1d(a INT[]);
INSERT INTO int_array_1d SELECT CAST(a as INT[]) FROM (VALUES
    ('{1, 2, 3}'),
    (NULL),
    ('{4, 5, NULL, 7}'),
    ('{}')
) t(a);
SELECT * FROM int_array_1d;

-- INT4 (two dimensional data, single dimension type)
CREATE TABLE int_array_2d(a INT[]);
INSERT INTO int_array_2d VALUES
    ('{{1, 2}, {3, 4}}'),
    ('{{5, 6, 7}, {8, 9, 10}}'),
    ('{{11, 12, 13}, {14, 15, 16}}'),
    ('{{17, 18}, {19, 20}}');
SELECT * FROM int_array_2d;
drop table int_array_2d;

-- INT4 (single dimensional data, two dimensionsal type)
CREATE TABLE int_array_2d(a INT[][]);
INSERT INTO int_array_2d VALUES
    ('{1, 2}'),
    ('{5, 6, 7}'),
    ('{11, 12, 13}'),
    ('{17, 18}');
SELECT * FROM int_array_2d;
drop table int_array_2d;

-- INT4 (two dimensional data and type)
CREATE TABLE int_array_2d(a INT[][]);
INSERT INTO int_array_2d VALUES
    ('{{1, 2}, {3, 4}}'),
    ('{{5, 6, 7}, {8, 9, 10}}'),
    ('{{11, 12, 13}, {14, 15, 16}}'),
    ('{{17, 18}, {19, 20}}');
SELECT * FROM int_array_2d;

-- INT8 (single dimension)
CREATE TABLE bigint_array_1d(a BIGINT[]);
INSERT INTO bigint_array_1d SELECT CAST(a as BIGINT[]) FROM (VALUES
    ('{9223372036854775807, 2, -9223372036854775808}'),
    (NULL),
    ('{4, 4294967296, NULL, 7}'),
    ('{}')
) t(a);
SELECT * FROM bigint_array_1d;

-- BOOL (single dimension)
CREATE TABLE bool_array_1d(a BOOL[]);
INSERT INTO bool_array_1d SELECT CAST(a as BOOL[]) FROM (VALUES
    ('{true, false, true}'),
    (NULL),
    ('{true, true, NULL, false}'),
    ('{}')
) t(a);
SELECT * FROM bool_array_1d;

DROP TABLE int_array_0d;
DROP TABLE int_array_1d;
DROP TABLE int_array_2d;
DROP TABLE bigint_array_1d;
DROP TABLE bool_array_1d;
