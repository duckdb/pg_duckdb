create table hugeint_sum(a int);
insert into hugeint_sum select g from generate_series(1,100) g;
select pg_typeof(sum(a)) from hugeint_sum;
select sum(a) result from hugeint_sum;

drop table hugeint_sum;

-- the minimum hugeint cannot be negated as a signed value, the conversion must not overflow
select * from duckdb.query($$ select '-170141183460469231731687303715884105728'::hugeint as result $$);
select * from duckdb.query($$ select '-170141183460469231731687303715884105727'::hugeint as result $$);
select * from duckdb.query($$ select '170141183460469231731687303715884105727'::hugeint as result $$);
select * from duckdb.query($$ select '-170141183460469231731687303715884105.28'::decimal(38,2) as result $$);
select * from duckdb.query($$ select '-3276.8'::decimal(5,1) as result $$);
select * from duckdb.query($$ select '-9223372036854775808'::bigint as result $$);
