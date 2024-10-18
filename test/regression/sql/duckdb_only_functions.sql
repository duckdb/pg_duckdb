-- We should be able to use duckdb-only functions even when
-- duckdb.force_execution is turned off
SET duckdb.force_execution = false;

\getenv pwd PWD

select * from read_parquet(:'pwd' || '/data/unsigned_types.parquet') as (usmallint int);
select * from read_parquet(ARRAY[:'pwd' || '/data/unsigned_types.parquet']) as (usmallint int);

select * from read_csv(:'pwd' || '/data/web_page.csv') as (column00 int) LIMIT 2;
select * from read_csv(ARRAY[:'pwd' || '/data/web_page.csv']) as (column00 int) LIMIT 2;

-- TODO: Add a test for scan_iceberg once we have a test table
