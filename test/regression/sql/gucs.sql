show duckdb.memory_limit;
select * from duckdb.query($$ select current_setting('memory_limit')$$);
CALL duckdb.recycle_ddb();

set duckdb.memory_limit = 1024;
select * from duckdb.query($$ select current_setting('memory_limit') $$);

-- Don't recycle DuckDB, make sure we get an error:
set duckdb.memory_limit = '1GB';

set duckdb.threads = 42;

set duckdb.autoinstall_known_extensions = false;

-- The built-in defaults of the directory GUCs used to point to freed memory
-- in backends, which made pg_settings return invalid UTF-8 (#1005)
SELECT name, boot_val = setting AS boot_val_is_setting FROM pg_settings
WHERE name IN ('duckdb.temporary_directory', 'duckdb.extension_directory') ORDER BY name;
