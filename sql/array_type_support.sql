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

DROP TABLE int_array_1d;
