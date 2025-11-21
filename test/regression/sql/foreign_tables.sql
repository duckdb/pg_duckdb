-- parquet
CREATE FOREIGN TABLE external_parquet ()
  SERVER ddb_foreign_server
  OPTIONS (
    location '../../data/iris.parquet',
    format   'parquet',
    options  '{"filename": true, "file_row_number": true}'
  );

SELECT "sepal.length", "file_row_number", "filename" FROM external_parquet ORDER BY "sepal.length", "file_row_number" LIMIT 5;
SELECT count(*) FROM external_parquet;

ALTER TABLE external_parquet RENAME TO external_parquet_renamed;
SELECT count(*) FROM external_parquet_renamed;

ALTER FOREIGN TABLE external_parquet_renamed RENAME TO external_parquet_renamed_1;
SELECT count(*) FROM external_parquet_renamed_1;

CREATE SCHEMA external_parquet_schema;
SET search_path to external_parquet_schema;
CREATE FOREIGN TABLE external_parquet ()
  SERVER ddb_foreign_server
  OPTIONS (
    location '../../data/iris.parquet'
  );
SELECT count(*) FROM external_parquet;
DROP FOREIGN TABLE external_parquet;
RESET search_path;

-- CSV
CREATE FOREIGN TABLE external_csv ()
  SERVER ddb_foreign_server
  OPTIONS (
    location '../../data/iris.csv',
    format   'csv',
    options  '{"header": true}'
  );

SELECT count(*) FROM external_csv;
SELECT min("sepal.length"), max("sepal.length") FROM external_csv;

-- JSON
CREATE FOREIGN TABLE external_json ()
  SERVER ddb_foreign_server
  OPTIONS (
    location '../../data/table.json',
    format   'json'
  );

SELECT count(*) FROM external_json;
SELECT sum(a), min(b), max(c) FROM external_json;

-- CTAS from foreign table
CREATE TABLE json_tbl AS SELECT * FROM external_json;
SELECT count(*) FROM json_tbl;
SELECT sum(a), min(b), max(c) FROM json_tbl;

DROP FOREIGN TABLE external_parquet_renamed_1;
DROP FOREIGN TABLE external_csv;
DROP FOREIGN TABLE external_json;
DROP TABLE json_tbl;
