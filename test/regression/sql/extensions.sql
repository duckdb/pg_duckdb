
SET duckdb.execution TO false;

SELECT * FROM duckdb.raw_query($$ SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE loaded $$);

SELECT last_value FROM duckdb.extensions_table_seq;

-- INSERT SHOULD TRIGGER UPDATE OF EXTENSIONS

INSERT INTO duckdb.extensions (name, enabled) VALUES ('icu', TRUE);

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE loaded $$);

INSERT INTO duckdb.extensions (name, enabled) VALUES ('aws', TRUE);

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE loaded $$);

-- DELETE SHOULD TRIGGER UPDATE OF EXTENSIONS
-- But we do not unload for now (would require a restart of DuckDB)
DELETE FROM duckdb.extensions WHERE name = 'aws';

SELECT last_value FROM duckdb.extensions_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT extension_name, loaded, installed FROM duckdb_extensions() WHERE loaded $$);

SET duckdb.execution TO true;
