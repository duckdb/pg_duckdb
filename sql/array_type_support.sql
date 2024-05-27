drop extension if exists quack;
create extension quack;

-- INT4 (single dimension)
CREATE TABLE int_array_1d(a INT[]);
INSERT INTO int_array_1d SELECT CAST(a as INT[]) FROM (VALUES
    ('{1, 2, 3}'),
    ('{4, 5, NULL, 7}'),
    (NULL),
    ('{}')
) t(a);
SELECT * FROM int_array_1d;

-- BOOL (single dimension)
CREATE TABLE bool_array_1d(a BOOL[]);
INSERT INTO bool_array_1d SELECT CAST(a as BOOL[]) FROM (VALUES
    ('{true, false, true}'),
    ('{true, true, NULL, false}'),
    (NULL),
    ('{}')
) t(a);
SELECT * FROM bool_array_1d;

DROP TABLE int_array_1d;
DROP TABLE bool_array_1d;
