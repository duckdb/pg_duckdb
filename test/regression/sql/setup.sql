-- Install httpfs from the extension repository of the DuckDB build, so that
-- the tests use it instead of downloading one. Static builds link it in.
\set repository `echo "$PG_DUCKDB_EXTENSION_REPOSITORY"`
SELECT duckdb.install_extension('httpfs', :'repository');
SELECT r['install_mode'] = 'STATICALLY_LINKED' OR r['installed_from'] = :'repository' AS httpfs_from_build
FROM duckdb.query($$ SELECT install_mode, installed_from FROM duckdb_extensions() WHERE extension_name = 'httpfs' $$) r;
