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

-- The min aggregate was somehow missing from the list of aggregates in 0.3.0
CREATE AGGREGATE @extschema@.min(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE FUNCTION @extschema@.strftime(date, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strftime(timestamp, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strftime(timestamptz, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strftime(duckdb.unresolved_type, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strptime(text, text) RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strptime(duckdb.unresolved_type, text) RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strptime(text, text[]) RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.strptime(duckdb.unresolved_type, text[]) RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(interval) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(date) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(timestamp) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(timestamptz) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(time) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(timetz) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch(duckdb.unresolved_type) RETURNS double
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(interval) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(date) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(timestamp) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(timestamptz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(time) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(timetz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(bigint) RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ms(duckdb.unresolved_type) RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(interval) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(date) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(timestamp) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(timestamptz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(time) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(timetz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_us(duckdb.unresolved_type) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(interval) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(date) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(timestamp) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(timestamptz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(time) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(timetz) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.epoch_ns(duckdb.unresolved_type) RETURNS bigint
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.date_trunc(text, duckdb.unresolved_type) RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.length(duckdb.unresolved_type) RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.regexp_replace(duckdb.unresolved_type, text, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.regexp_replace(duckdb.unresolved_type, text, text, text) RETURNS text
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts date)
RETURNS date
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts date, origin date)
RETURNS date
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts date, time_offset interval)
RETURNS date
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp)
RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp, time_offset interval)
RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp, origin timestamp)
RETURNS timestamp
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp with time zone)
RETURNS timestamp with time zone
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp with time zone, time_offset interval)
RETURNS timestamp with time zone
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp with time zone, origin timestamp with time zone)
RETURNS timestamp with time zone
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts timestamp with time zone, timezone varchar)
RETURNS timestamp with time zone
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type, time_offset interval)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type, origin date)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type, origin timestamp)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type, origin timestamp with time zone)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.time_bucket(bucket_width interval, ts duckdb.unresolved_type, timezone varchar)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE CAST (duckdb.unresolved_type AS interval)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS interval[])
    WITH INOUT;

CREATE CAST (duckdb.unresolved_type AS time)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS time[])
    WITH INOUT;

CREATE CAST (duckdb.unresolved_type AS timetz)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS timetz[])
    WITH INOUT;

CREATE CAST (duckdb.unresolved_type AS bit)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS bit[])
    WITH INOUT;

CREATE CAST (duckdb.unresolved_type AS bytea)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS bytea[])
    WITH INOUT;

CREATE OPERATOR pg_catalog.~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~ (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~ (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~ (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~* (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~* (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.~~* (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~ (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~ (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~* (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~* (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR pg_catalog.!~~* (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE TYPE duckdb.union;
CREATE FUNCTION duckdb.union_in(cstring) RETURNS duckdb.union AS 'MODULE_PATHNAME', 'duckdb_union_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.union_out(duckdb.union) RETURNS cstring AS 'MODULE_PATHNAME', 'duckdb_union_out' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE duckdb.union(
    INTERNALLENGTH = VARIABLE,
    INPUT = duckdb.union_in,
    OUTPUT = duckdb.union_out
);

CREATE FUNCTION @extschema@.union_extract(union_col duckdb.unresolved_type, tag text)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.union_extract(union_col duckdb.union, tag text)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.union_tag(union_col duckdb.unresolved_type)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION @extschema@.union_tag(union_col duckdb.union)
RETURNS duckdb.unresolved_type
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION duckdb.is_motherduck_enabled()
RETURNS bool
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_is_motherduck_enabled';
REVOKE ALL ON FUNCTION duckdb.is_motherduck_enabled() FROM PUBLIC;

CREATE FUNCTION pgduckdb_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'pgduckdb_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION pgduckdb_fdw_validator(
    options text[],
    catalog oid
)
RETURNS void
AS 'MODULE_PATHNAME', 'pgduckdb_fdw_validator'
LANGUAGE C STRICT PARALLEL SAFE;

CREATE FOREIGN DATA WRAPPER duckdb
  HANDLER pgduckdb_fdw_handler
  VALIDATOR pgduckdb_fdw_validator;

CREATE FUNCTION duckdb.enable_motherduck(TEXT DEFAULT '::FROM_ENV::', TEXT DEFAULT '')
RETURNS bool
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_enable_motherduck';

CREATE TYPE duckdb.map;
CREATE FUNCTION duckdb.map_in(cstring) RETURNS duckdb.map AS 'MODULE_PATHNAME', 'duckdb_map_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.map_out(duckdb.map) RETURNS cstring AS 'MODULE_PATHNAME', 'duckdb_map_out' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE duckdb.map(
    INTERNALLENGTH = VARIABLE,
    INPUT = duckdb.map_in,
    OUTPUT = duckdb.map_out
);

-- Drop legacy secret objects
DROP SEQUENCE duckdb.secrets_table_seq;

-- CASCADE will drop the following triggers:
-- DROP TRIGGER duckdb_secret_r2_tr;
-- DROP TRIGGER secrets_table_seq_tr;
DROP TABLE duckdb.secrets CASCADE;

DROP FUNCTION duckdb.duckdb_secret_r2_check();
DROP FUNCTION duckdb.duckdb_update_secrets_table_seq();

-- Secrets helpers
CREATE FUNCTION duckdb.create_simple_secret(
    TEXT,              -- Type (S3, GCS, R2)
    TEXT,              -- Key Id
    TEXT,              -- Secret
    TEXT DEFAULT '',   -- Session Token
    TEXT DEFAULT 'us-east-1'
)
RETURNS TEXT
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_create_simple_secret';

CREATE FUNCTION duckdb.create_azure_secret(TEXT) -- connection string
RETURNS TEXT
SET search_path = pg_catalog, pg_temp
LANGUAGE C AS 'MODULE_PATHNAME', 'pgduckdb_create_azure_secret';


