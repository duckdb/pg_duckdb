-- All functions should automatically use duckdb
set duckdb.execution = false;
select r['a'] ~ 'a.*' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] !~ 'x.*' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] LIKE '%b%' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] NOT LIKE '%x%' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] ILIKE '%B%' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] NOT ILIKE '%X%' from duckdb.query($$ SELECT 'abc' a $$) r;
-- Currently not working, we need to add the similar_to_escape function to
-- DuckDB to make this work.
select r['a'] SIMILAR TO '%b%' from duckdb.query($$ SELECT 'abc' a $$) r;
select r['a'] NOT SIMILAR TO '%b%' from duckdb.query($$ SELECT 'abc' a $$) r;
select length(r['a']) from duckdb.query($$ SELECT 'abc' a $$) r;
select regexp_replace(r['a'], 'a', 'x') from duckdb.query($$ SELECT 'abc' a $$) r;
select regexp_replace(r['a'], 'A', 'x', 'i') from duckdb.query($$ SELECT 'abc' a $$) r;
select date_trunc('year', r['ts']) from duckdb.query($$ SELECT '2024-08-04 12:34:56'::timestamp ts $$) r;

select strptime('Wed, 1 January 1992 - 08:38:40 PM', '%a, %-d %B %Y - %I:%M:%S %p');
select strptime('4/15/2023 10:56:00', ARRAY['%d/%m/%Y %H:%M:%S', '%m/%d/%Y %H:%M:%S']);
select strftime(date '1992-01-01', '%a, %-d %B %Y - %I:%M:%S %p');
select strftime(timestamp '1992-01-01 20:38:40', '%a, %-d %B %Y - %I:%M:%S %p');
select strftime(timestamptz '1992-01-01 20:38:40', '%a, %-d %B %Y - %I:%M:%S %p');
select strftime(r['ts'], '%a, %-d %B %Y - %I:%M:%S %p') from duckdb.query($$ SELECT timestamp '1992-01-01 20:38:40' ts $$) r;
