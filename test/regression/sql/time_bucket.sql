-- All functions should automatically use duckdb
set duckdb.execution = false;

SELECT time_bucket('1 day'::interval, '2023-01-15'::date);

SELECT time_bucket('1 month'::interval, '2023-01-15'::date, '2022-12-25'::date);

SELECT time_bucket('1 week'::interval, '2023-01-15'::date, '2 days'::interval);

SELECT time_bucket('1 hour'::interval, '2023-01-15 14:30:00'::timestamp);

SELECT time_bucket('4 hours'::interval, '2023-01-15 14:30:00'::timestamp, '1 hour'::interval);

SELECT time_bucket('1 day'::interval, '2023-01-15 14:30:00'::timestamp, '2022-12-25 06:00:00'::timestamp);

SELECT time_bucket('12 hours'::interval, '2023-01-15 14:30:00+00'::timestamp with time zone);

SELECT time_bucket('1 day'::interval, '2023-01-15 14:30:00+00'::timestamp with time zone, '6 hours'::interval);

SELECT time_bucket('1 week'::interval, '2023-01-15 14:30:00+00'::timestamp with time zone, '2022-12-25 12:00:00+00'::timestamp with time zone);

SELECT time_bucket('1 day'::interval, '2023-01-15 14:30:00+00'::timestamp with time zone, 'America/New_York');
