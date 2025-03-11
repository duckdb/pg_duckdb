-- All functions should automatically use duckdb
set duckdb.execution = false;

CREATE TABLE dates(w INTERVAL, d DATE, shift INTERVAL, origin DATE);

INSERT INTO dates VALUES
	('10 days', '0794-11-15', '1 week', '0790-11-01'),
	('10 days', '1700-01-01', '0 days', '1700-01-05'),
	('1 week', '1832-05-03', '0 days', '1970-05-05'),
	('10 days', '1897-07-05', '2 days', '1970-06-07'),
	('2 months', '1946-09-14', '0 months', '1970-07-05'),
	('2 months', '2000-01-01', '1 month 1 week', '1970-05-01'),
	('2 year', '2004-05-20', '6 months', '1970-12-31');

select d, time_bucket('3 days'::interval, d) from dates;

select d, time_bucket('3 years'::interval, d) from dates;

select d, time_bucket(null::interval, d) from dates;

select w, d, time_bucket(w, d) from dates;

select d, time_bucket('4 days'::interval, d, '6 hours'::interval) from dates;

select d, time_bucket('2 weeks'::interval, d, '6 days'::interval) from dates;

select d, time_bucket('3 months'::interval, d, '6 days'::interval) from dates;

select d, time_bucket(null::interval, d, '6 days'::interval) from dates;

select time_bucket('3 months'::interval, null::date, '6 days'::interval) from dates;

select d, time_bucket('3 months'::interval, d, null::interval) from dates;

select w, d, shift, time_bucket(w, d, shift) from dates;

select d, time_bucket('5 days'::interval, d, '1970-01-04'::date) from dates;

select d, time_bucket('3 months'::interval, d, '1970-01-04'::date) from dates;

select d, time_bucket('3 years'::interval, d, '1970-01-04'::date) from dates;

select d, time_bucket(null::interval, d, '1970-01-04'::date) from dates;

select time_bucket('3 years'::interval, null::date, '1970-01-04'::date) from dates;

select d, time_bucket('3 years'::interval, d, null::date) from dates;

select w, d, origin, time_bucket(w, d, origin) from dates;

select time_bucket('-3 hours'::interval, '2019-04-05'::date);

select time_bucket('-3 hours'::interval, '2019-04-05'::date, '1 hour 30 minutes'::interval);

select time_bucket('-3 hours'::interval, '2019-04-05'::date, '2019-04-05'::date);

select time_bucket('-1 month'::interval, '2019-04-05'::date);

select time_bucket('-1 month'::interval, '2019-04-05'::date, '1 week'::interval);

select time_bucket('-1 month'::interval, '2019-04-05'::date, '2019-04-05'::date);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05'::date);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05'::date, '1 hour 30 minutes'::interval);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05'::date, '2018-05-05'::date);

select time_bucket('1 month 1 day'::interval, '2018-05-05'::date);

select time_bucket('1 month 1 day'::interval, '2018-05-05'::date, '1 week'::interval);

select time_bucket('1 month 1 day'::interval, '2019-05-05'::date, '2019-05-05'::date);

select time_bucket('3 days'::interval, '2019-05-05'::date, '2000000000 months'::interval);

select time_bucket('3 days'::interval, '2019-05-05'::date, '-2000000000 months'::interval);

select time_bucket('3 months'::interval, '2019-05-05'::date, '2000000000 months'::interval);

select time_bucket('3 months'::interval, '2019-05-05'::date, '-2000000000 months'::interval);

select time_bucket('1 month'::interval, '5877642-06-25 (BC)'::date);

select time_bucket('1 month'::interval, '5877642-07-01 (BC)'::date);

select time_bucket('1 week'::interval, '5877642-07-01 (BC)'::date);

select time_bucket('1 month'::interval, '5881580-07-10'::date, '-1 day'::interval);

select time_bucket('1 month'::interval, '5881580-07-10'::date);

select time_bucket('1 week'::interval, '290309-12-21 (BC)'::date);

select time_bucket('1 week'::interval, '290309-12-22 (BC)'::date);

select time_bucket('1 day'::interval, '290309-12-21 (BC)'::date);

select time_bucket('1 day'::interval, '290309-12-22 (BC)'::date);

select time_bucket('1 week'::interval, '294247-01-11'::date);

select time_bucket('1 week'::interval, '294247-01-10'::date);

select time_bucket('1 day'::interval, '294247-01-11'::date);

select time_bucket('1 day'::interval, '294247-01-10'::date);

select time_bucket('1 month 1 day'::interval, null::date);

select time_bucket('1 month 1 day'::interval, null::date, '6 days'::interval);

select time_bucket('1 month 1 day'::interval, null::date, '2022-12-20'::date);

select time_bucket('-1 month'::interval, null::date);

select time_bucket('-1 month'::interval, null::date, '6 days'::interval);

select time_bucket('-1 month'::interval, '2022-12-22'::date, null::interval);

select time_bucket('-1 month'::interval, null::date, '2022-12-20'::date);

select time_bucket('-1 month'::interval, '2022-12-22'::date, null::date);

CREATE TABLE timestamps(w INTERVAL, t TIMESTAMP, shift INTERVAL, origin TIMESTAMP);

INSERT INTO timestamps VALUES ('10 days', '-infinity', '0 days', '1970-01-05 00:00:00'),
	('10 days', '3000-01-02 (BC) 03:16:23.003003', '3 days', '3000-01-01 (BC) 00:00:00'),
	('2 months', '1024-04-10 (BC) 12:35:40.003003', '10 days', '1024-03-01 (BC) 00:00:00'),
	('10 days', '0044-06-15 (BC) 12:35:40.003003', '6 days', '0044-02-01 (BC) 00:00:00'),
	('333 microseconds', '0678-06-30 02:02:03.003003', '0 microseconds', '1970-02-10 00:00:00'),
	('333 microseconds', '0794-07-03 02:02:04.003003', '444 microseconds', '1970-10-17 00:05:05.006006'),
	('333 microseconds', '1700-01-01 00:00:00', '-444 microseconds', '1970-09-27 00:05:05.006006'),
	('333 milliseconds', '1962-12-31 00:00:00', '0 milliseconds', '1970-08-12 00:00:00'),
	('333 milliseconds', '1970-01-01 00:00:00', '444 milliseconds', '1970-06-23 00:05:05.006006'),
	('333 milliseconds', '1985-12-07 02:02:08.003003', '-444 milliseconds', '1970-01-05 00:05:05.006006'),
	('333 seconds', '1989-10-18 02:02:09.003003', '0 seconds', '1970-09-07 00:00:00'),
	('333 seconds', '1990-01-21 02:02:10.003003', '444 seconds', '1970-07-06 00:05:05.006006'),
	('333 seconds', '1991-02-10 02:02:11.003003', '-444 seconds', '1970-10-09 00:05:05.006006'),
	('333 minutes', '1992-09-11 02:02:12.003003', '0 minutes', '1970-04-10 00:00:00'),
	('333 minutes', '1994-12-26 02:02:13.003003', '444 minute', '1970-03-05 00:05:05.006006'),
	('333 minutes', '1997-05-13 02:02:14.003003', '-444 minute', '2000-01-03 00:00:00'),
	('333 hours', '1999-02-14 02:02:15.003003', '0 hours', '2000-01-01 00:00:00'),
	('333 hours', '2000-01-01 00:00:00', '444 hours', '1980-11-25 00:05:05.006006'),
	('333 hours', '2000-01-03 00:00:00', '-444 hours', '2045-01-05 00:05:05.006006'),
	('10 days', '2008-09-17 02:02:18.003003', '0 days', '2009-11-05 00:00:00'),
	('1 week', '2010-08-18 02:02:19.003003', '0 days', '2100-09-05 00:00:00'),
	('10 days', '2013-03-19 02:02:20.003003', '2 days 4 hours', '2300-10-07 00:05:05.006006'),
	('10 days', '2014-11-20 02:02:21.003003', '-2 days 4 hours', '1970-12-07 00:05:05.006006'),
	('2 months', '2016-02-21 02:02:22.003003', '0 months', '1970-09-05 00:00:00'),
	('2 months', '2018-08-22 02:02:23.003003', '1 month 1 week', '1970-07-01 00:05:05.006006'),
	('2 months', '2019-01-23 02:02:24.003003', '-1 month 1 week', '1969-09-01 00:05:05.006006'),
	('2 year', '2020-02-24 02:02:25.003003', '6 months', '1970-02-13 00:05:05.006006'),
	('2 year', '2022-07-25 02:02:26.003003', '-6 months', '1969-10-09 00:05:05.006006'),
	('10 days', '2024-02-25 02:02:26.003003', '1 year', '-infinity'),
	('10 days', '2032-09-25 02:02:26.003003', '-1 year', 'infinity'),
	('10 days', 'infinity', '0 days', '1970-01-01 00:00:00');

select t, time_bucket('56 seconds'::interval, t) from timestamps;

select t, time_bucket('3 days'::interval, t) from timestamps;

select t, time_bucket('3 years'::interval, t) from timestamps;

select t, time_bucket(null::interval, t) from timestamps;

select time_bucket('3 years'::interval, null::timestamp) from timestamps;

select w, t, time_bucket(w, t) from timestamps;

select t, time_bucket('4 seconds'::interval, t, '2 seconds'::interval) from timestamps;

select t, time_bucket('4 days'::interval, t, '6 hours'::interval) from timestamps;

select t, time_bucket('3 months'::interval, t, '6 days 11 hours'::interval) from timestamps;

select t, time_bucket(null::interval, t, '2 seconds'::interval) from timestamps;

select time_bucket('3 months'::interval, null::timestamp, '2 seconds'::interval) from timestamps;

select t, time_bucket('3 months'::interval, t, null::interval) from timestamps;

select w, t, shift, time_bucket(w, t, shift) from timestamps;

select t, time_bucket('11 seconds'::interval, t, '1990-12-10 08:08:10'::timestamp) from timestamps;

select t, time_bucket('11 days'::interval, t, '1990-01-06 08:08:10'::timestamp) from timestamps;

select t, time_bucket('7 months'::interval, t, '1990-01-06 08:08:10'::timestamp) from timestamps;

select t, time_bucket(null::interval, t, '1990-01-06 08:08:10'::timestamp) from timestamps;

select time_bucket('7 months'::interval, null::timestamp, '1990-01-06 08:08:10'::timestamp) from timestamps;

select t, time_bucket('7 months'::interval, t, null::timestamp) from timestamps;

select w, t, origin, time_bucket(w, t, origin) from timestamps;

select time_bucket('-3 hours'::interval, '2019-04-05 00:00:00'::timestamp);

select time_bucket('-3 hours'::interval, '2019-04-05 00:00:00'::timestamp, '1 hour 30 minutes':: interval);

select time_bucket('-3 hours'::interval, '2019-04-05 00:00:00'::timestamp, '2019-04-05 00:00:00'::timestamp);

select time_bucket('-1 month'::interval, '2019-04-05 00:00:00'::timestamp);

select time_bucket('-1 month'::interval, '2019-04-05 00:00:00'::timestamp, '1 hour 30 minutes':: interval);

select time_bucket('-1 month'::interval, '2019-04-05 00:00:00'::timestamp, '2018-04-05 00:00:00'::timestamp);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05 00:00:00'::timestamp);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05 00:00:00'::timestamp, '1 hour 30 minutes':: interval);

select time_bucket('1 day - 172800 seconds'::interval, '2018-05-05 00:00:00'::timestamp, '2018-05-05 00:00:00'::timestamp);

select time_bucket('1 month 1 day'::interval, '2018-05-05 00:00:00'::timestamp);

select time_bucket('1 month 1 day'::interval, '2018-05-05 00:00:00'::timestamp, '1 hour 30 minutes':: interval);

select time_bucket('1 month 1 day'::interval, '2018-05-05 00:00:00'::timestamp, '2018-05-05 00:00:00'::timestamp);

select time_bucket('3 days'::interval, '2019-05-05 00:00:00'::timestamp, '2000000000 months'::interval);

select time_bucket('3 days'::interval, '2019-05-05 00:00:00'::timestamp, '-2000000000 months'::interval);

select time_bucket('3 months'::interval, '2019-05-05 00:00:00'::timestamp, '2000000000 months'::interval);

select time_bucket('3 months'::interval, '2019-05-05 00:00:00'::timestamp, '-2000000000 months'::interval);

select time_bucket('1 microseconds'::interval, '290309-12-21 (BC) 23:59:59.999999'::timestamp);

select time_bucket('1 microseconds'::interval, '290309-12-22 (BC) 00:00:00'::timestamp);

select time_bucket('1 microseconds'::interval, '294247-01-10 04:00:54.775807'::timestamp);

select time_bucket('1 microseconds'::interval, '294247-01-10 04:00:54.775806'::timestamp);

select time_bucket('1 month 1 day'::interval, null::timestamp);

select time_bucket('1 month 1 day'::interval, null::timestamp, '6 days'::interval);

select time_bucket('1 month 1 day'::interval, null::timestamp, '2022-12-20 10:00:00'::timestamp);

select time_bucket('-1 month'::interval, null::timestamp);

select time_bucket('-1 month'::interval, null::timestamp, '6 days'::interval);

select time_bucket('-1 month'::interval, '2022-12-22 10:00:00'::timestamp, null::interval);

select time_bucket('-1 month'::interval, null::timestamp, '2022-12-20 10:00:00'::timestamp);

select time_bucket('-1 month'::interval, '2022-12-22'::timestamp, null::timestamp);

drop table dates, timestamps;