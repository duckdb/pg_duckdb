
SET duckdb.force_execution TO false;
SET duckdb.allow_community_extensions = true;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);

SELECT last_value FROM duckdb.extensions_table_seq;

-- INSERT SHOULD TRIGGER UPDATE OF EXTENSIONS

SELECT duckdb.install_extension('icu');

-- Increases the sequence twice because we use ON CONFLICT DO UPDATE. So
-- the trigger fires for both INSERT and UPDATE internally.
SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);

-- Check that we can rerun this without issues
SELECT duckdb.install_extension('icu');

-- Increases the sequence twice because we use ON CONFLICT DO UPDATE. So
-- the trigger fires for both INSERT and UPDATE internally.
SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);


SELECT duckdb.install_extension('aws');

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);

-- DELETE SHOULD TRIGGER UPDATE OF EXTENSIONS
-- But we do not unload for now (would require a restart of DuckDB)
DELETE FROM duckdb.extensions WHERE name = 'aws';

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);

SELECT duckdb.install_extension('prql', 'community');

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.query($$ SELECT extension_name, loaded, installed, installed_from FROM duckdb_extensions() WHERE loaded and extension_name != 'jemalloc' $$);

-- cleanup
TRUNCATE duckdb.extensions;
