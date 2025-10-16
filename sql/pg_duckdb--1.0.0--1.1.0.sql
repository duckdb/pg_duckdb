DROP FUNCTION duckdb.create_simple_secret(
    type          TEXT,
    key_id        TEXT,
    secret        TEXT,
    session_token TEXT,
    region        TEXT,
    url_style     TEXT,
    provider      TEXT,
    endpoint      TEXT,
    scope         TEXT
);

CREATE FUNCTION duckdb.create_simple_secret(
    type          TEXT, -- One of (S3, GCS, R2)
    key_id        TEXT,
    secret        TEXT,
    session_token TEXT DEFAULT '',
    region        TEXT DEFAULT '',
    url_style     TEXT DEFAULT '',
    provider      TEXT DEFAULT '',
    endpoint      TEXT DEFAULT '',
    scope         TEXT DEFAULT '',
    validation    TEXT DEFAULT '',
    use_ssl       TEXT DEFAULT ''
)
RETURNS TEXT
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_create_simple_secret';
