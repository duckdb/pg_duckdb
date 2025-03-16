show duckdb.memory_limit;
select * from duckdb.query($$ select current_setting('memory_limit') == '3.7 GiB' $$);

set duckdb.memory_limit = '1GiB';
CALL duckdb.recycle_ddb();
select * from duckdb.query($$ select current_setting('memory_limit') $$);

set duckdb.memory_limit = '';
CALL duckdb.recycle_ddb();
select * from duckdb.query($$ select current_setting('memory_limit') != '3.7 GiB' $$);
