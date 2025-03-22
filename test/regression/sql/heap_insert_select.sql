
-- CSV
CREATE TABLE heap_table (sepal_length float, variety varchar);
INSERT INTO heap_table SELECT r['sepal.length']::float, r['variety']::varchar FROM read_csv('../../data/iris.csv') r;
SELECT sepal_length, variety FROM heap_table ORDER BY sepal_length LIMIT 5;
DROP TABLE heap_table;

-- JSON
CREATE TABLE heap_table (a int PRIMARY KEY, b varchar, c real);
INSERT INTO heap_table SELECT r['a']::int, r['b']::varchar, r['c']::real FROM read_json('../../data/table.json') r;
SELECT * FROM heap_table ORDER BY a LIMIT 5;
-- DO IT AGAIN TO VALIDATE PK CONSTRAINT
INSERT INTO heap_table SELECT r['a']::int, r['b']::varchar, r['c']::real FROM read_json('../../data/table.json') r;
