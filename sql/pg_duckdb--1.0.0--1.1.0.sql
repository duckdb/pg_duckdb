CREATE TABLE IF NOT EXISTS duckdb.external_tables (
    relid regclass PRIMARY KEY,
    reader TEXT NOT NULL,
    location TEXT NOT NULL,
    options JSONB
);

REVOKE ALL ON duckdb.external_tables FROM PUBLIC;
