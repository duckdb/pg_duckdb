-- parquet
CREATE TABLE external_parquet () USING duckdb WITH (
    duckdb_external_location = '../../data/iris.parquet',
    duckdb_external_reader = 'read_parquet',
    duckdb_external_options = '{"filename": true, "file_row_number": true}'
);

SELECT "sepal.length", "file_row_number", "filename" FROM external_parquet ORDER BY "sepal.length", "file_row_number" LIMIT 5;
SELECT count(*) FROM external_parquet;

ALTER TABLE external_parquet RENAME TO external_parquet_renamed;
SELECT count(*) FROM external_parquet_renamed;

CREATE SCHEMA external_parquet_schema;
SET search_path to external_parquet_schema;
CREATE TABLE external_parquet () USING duckdb WITH (
    duckdb_external_location = '../../data/iris.parquet',
    duckdb_external_reader = 'read_parquet'
);
SELECT count(*) FROM external_parquet;
DROP TABLE external_parquet;
RESET search_path;

-- CSV
CREATE TABLE external_csv () USING duckdb WITH (
    duckdb_external_location = '../../data/iris.csv',
    duckdb_external_format = 'csv',
    duckdb_external_options = '{"header": true}'
);

SELECT count(*) FROM external_csv;
SELECT min("sepal.length"), max("sepal.length") FROM external_csv;

-- JSON
CREATE TABLE external_json () USING duckdb WITH (
    duckdb_external_location = '../../data/table.json',
    duckdb_external_format = 'json'
);

SELECT count(*) FROM external_json;
SELECT sum(a), min(b), max(c) FROM external_json;

-- CTAS from external table
CREATE TABLE json_tbl AS SELECT * FROM external_json;
SELECT count(*) FROM json_tbl;
SELECT sum(a), min(b), max(c) FROM json_tbl;

DROP TABLE external_parquet_renamed;
DROP TABLE external_csv;
DROP TABLE external_json;
DROP TABLE json_tbl;

SELECT count(*) FROM duckdb.external_tables;