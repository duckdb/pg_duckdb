drop extension if exists quack;
create extension quack;

-- INT4 (single dimension)
CREATE TABLE int_array_1d(a INT[]);
INSERT INTO int_array_1d SELECT CAST(a as INT[]) FROM (VALUES
    ('{1, 2, 3}'),
    (NULL),
    ('{4, 5, NULL, 7}'),
    ('{}')
) t(a);
SELECT * FROM int_array_1d;

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

-- CHAR (single dimension)
CREATE TABLE char_array_1d(a CHAR[]);
INSERT INTO char_array_1d SELECT CAST(a as CHAR[]) FROM (VALUES
    ('{a, b, c}'),
    (NULL),
    ('{t, f, Z, A}'),
    ('{}')
) t(a);
SELECT * FROM char_array_1d;

DROP TABLE int_array_1d;
DROP TABLE bigint_array_1d;
DROP TABLE bool_array_1d;
DROP TABLE char_array_1d;
