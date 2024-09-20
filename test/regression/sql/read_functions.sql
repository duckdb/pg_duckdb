-- PostgreSQL instance has data directory set to tmp_check/data so for all read functions argument
-- is relative to that data directory patch

-- read_parquet

SELECT count("sepal.length") FROM read_parquet('../../data/iris.parquet') AS ("sepal.length" FLOAT);

SELECT "sepal.length" FROM read_parquet('../../data/iris.parquet') AS ("sepal.length" FLOAT) ORDER BY "sepal.length"  LIMIT 5;

SELECT "sepal.length", file_row_number, filename 
    FROM read_parquet('../../data/iris.parquet', file_row_number => true, filename => true) 
    AS ("sepal.length" FLOAT, file_row_number INT, filename VARCHAR) ORDER BY "sepal.length"  LIMIT 5;

-- read_csv

SELECT count("sepal.length") FROM read_csv('../../data/iris.csv') AS ("sepal.length" FLOAT);

SELECT "sepal.length" FROM read_csv('../../data/iris.csv') AS ("sepal.length" FLOAT) ORDER BY "sepal.length" LIMIT 5;

SELECT "sepal.length", filename
    FROM read_csv('../../data/iris.csv', filename => true, header => true) 
    AS ("sepal.length" FLOAT, filename VARCHAR) ORDER BY "sepal.length"  LIMIT 5;