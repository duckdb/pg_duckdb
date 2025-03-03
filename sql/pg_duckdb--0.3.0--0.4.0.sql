-- Add "url_style" column to "secrets" table
ALTER TABLE duckdb.secrets ADD COLUMN url_style TEXT;

DROP FUNCTION duckdb.cache_delete;
DROP FUNCTION duckdb.cache_info;
DROP FUNCTION duckdb.cache;
DROP TYPE duckdb.cache_info;

DROP FUNCTION duckdb.install_extension(TEXT);
CREATE FUNCTION duckdb.install_extension(extension_name TEXT, source TEXT DEFAULT 'core') RETURNS void
    SET search_path = pg_catalog, pg_temp
    LANGUAGE C AS 'MODULE_PATHNAME', 'install_extension';
REVOKE ALL ON FUNCTION duckdb.install_extension(TEXT, TEXT) FROM PUBLIC;
