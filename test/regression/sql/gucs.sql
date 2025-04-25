show duckdb.memory_limit;
select * from duckdb.query($$ select current_setting('memory_limit') == '3.7 GiB' $$);
CALL duckdb.recycle_ddb();

set duckdb.memory_limit = '1GiB';
select * from duckdb.query($$ select current_setting('memory_limit') $$);
CALL duckdb.recycle_ddb();

set duckdb.memory_limit = '';
select * from duckdb.query($$ select current_setting('memory_limit') != '3.7 GiB' $$);

-- Don't recycle DuckDB, make sure we get an error:
set duckdb.memory_limit = '1GiB';

set duckdb.threads = 42;

set duckdb.autoinstall_known_extensions = false;
