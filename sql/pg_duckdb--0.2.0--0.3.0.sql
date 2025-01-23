CREATE FUNCTION @extschema@.approx_count_distinct_sfunc(bigint, anyelement)
RETURNS bigint
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;


CREATE AGGREGATE @extschema@.approx_count_distinct(anyelement)
(
    sfunc = @extschema@.approx_count_distinct_sfunc,
    stype = bigint,
    initcond = 0
);

CREATE DOMAIN pg_catalog.blob AS bytea;
COMMENT ON DOMAIN pg_catalog.blob IS 'The DuckDB BLOB alias for BYTEA';

CREATE TYPE duckdb.row;
CREATE TYPE duckdb.unresolved_type;

-- TODO: Should we remove IMMUTABLE STRICT?
CREATE FUNCTION duckdb.row_in(cstring) RETURNS duckdb.row AS 'MODULE_PATHNAME', 'duckdb_row_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.row_out(duckdb.row) RETURNS cstring AS 'MODULE_PATHNAME', 'duckdb_row_out' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.row_subscript(internal) RETURNS internal AS 'MODULE_PATHNAME', 'duckdb_row_subscript' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE duckdb.row (
    INTERNALLENGTH = VARIABLE,
    INPUT = duckdb.row_in,
    OUTPUT = duckdb.row_out,
    SUBSCRIPT = duckdb.row_subscript
);

CREATE FUNCTION duckdb.unresolved_type_in(cstring) RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_in' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.unresolved_type_out(duckdb.unresolved_type) RETURNS cstring AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_out' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb.unresolved_type_subscript(internal) RETURNS internal AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_subscript' LANGUAGE C IMMUTABLE STRICT;
CREATE TYPE duckdb.unresolved_type (
    INTERNALLENGTH = VARIABLE,
    INPUT = duckdb.unresolved_type_in,
    OUTPUT = duckdb.unresolved_type_out,
    SUBSCRIPT = duckdb.unresolved_type_subscript
);

-- Dummy functions for binary operators with unresolved type on the lefthand
CREATE FUNCTION duckdb_unresolved_type_operator(duckdb.unresolved_type, "any") RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb_unresolved_type_operator_bool(duckdb.unresolved_type, "any") RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Dummy functions for binary operators with unresolved type on the righthand
CREATE FUNCTION duckdb_unresolved_type_operator("any", duckdb.unresolved_type) RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb_unresolved_type_operator_bool("any", duckdb.unresolved_type) RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Dummy functions for binary operators with unresolved type on both sides
CREATE FUNCTION duckdb_unresolved_type_operator(duckdb.unresolved_type, duckdb.unresolved_type) RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb_unresolved_type_operator_bool(duckdb.unresolved_type, duckdb.unresolved_type) RETURNS boolean AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Dummy function for prefix/unary operators
CREATE FUNCTION duckdb_unresolved_type_operator(duckdb.unresolved_type) RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- prefix operators + and -
CREATE OPERATOR + (
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR - (
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

-- Basic comparison operators
CREATE OPERATOR <= (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR <= (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR <= (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR < (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR < (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR < (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR <> (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR <> (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR <> (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR = (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR = (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR = (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR > (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR > (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR > (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR >= (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR >= (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator_bool
);

CREATE OPERATOR >= (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator_bool
);

-- binary math operators
CREATE OPERATOR + (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR + (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR + (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR - (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR - (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR - (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR * (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR * (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR * (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR / (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR / (
    LEFTARG = duckdb.unresolved_type,
    RIGHTARG = "any",
    FUNCTION = duckdb_unresolved_type_operator
);

CREATE OPERATOR / (
    LEFTARG = "any",
    RIGHTARG = duckdb.unresolved_type,
    FUNCTION = duckdb_unresolved_type_operator
);

-- TODO: use other dummy function with better error
CREATE FUNCTION duckdb_unresolved_type_btree_cmp(duckdb.unresolved_type, duckdb.unresolved_type) RETURNS int AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Create a B-tree operator class for duckdb.unresolved_type, so it can be used in ORDER BY
CREATE OPERATOR CLASS duckdb_unresolved_type_ops
DEFAULT FOR TYPE duckdb.unresolved_type USING btree AS
    OPERATOR 1 < (duckdb.unresolved_type, duckdb.unresolved_type),
    OPERATOR 2 <= (duckdb.unresolved_type, duckdb.unresolved_type),
    OPERATOR 3 = (duckdb.unresolved_type, duckdb.unresolved_type),
    OPERATOR 4 >= (duckdb.unresolved_type, duckdb.unresolved_type),
    OPERATOR 5 > (duckdb.unresolved_type, duckdb.unresolved_type),
    FUNCTION 1 duckdb_unresolved_type_btree_cmp(duckdb.unresolved_type, duckdb.unresolved_type);

CREATE FUNCTION duckdb_unresolved_type_hash(duckdb.unresolved_type) RETURNS int AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Create a hash operator class for duckdb.unresolved_type, so it can be used in GROUP BY
CREATE OPERATOR CLASS duckdb_unresolved_type_hash_ops
DEFAULT FOR TYPE duckdb.unresolved_type USING hash AS
    OPERATOR 1 = (duckdb.unresolved_type, duckdb.unresolved_type),
    FUNCTION 1 duckdb_unresolved_type_hash(duckdb.unresolved_type);

-- TODO: create dedicated dummy C functions for these
--
-- State transition function
CREATE FUNCTION duckdb_unresolved_type_state_trans(state duckdb.unresolved_type, value duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb_unresolved_type_state_trans(state duckdb.unresolved_type, value duckdb.unresolved_type, other "any")
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;
CREATE FUNCTION duckdb_unresolved_type_state_trans(state duckdb.unresolved_type, value duckdb.unresolved_type, other "any", another "any")
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Final function
CREATE FUNCTION duckdb_unresolved_type_final(state duckdb.unresolved_type)
RETURNS duckdb.unresolved_type AS 'MODULE_PATHNAME', 'duckdb_unresolved_type_operator' LANGUAGE C IMMUTABLE STRICT;

-- Aggregate functions

-- NOTE: any_value is already definied in core in PG16+, so we don't create it.
-- People using older Postgres versions can manually implement the aggregate if
-- they really require it.

CREATE AGGREGATE arbitrary(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_max(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_max(duckdb.unresolved_type, "any", "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_max_null(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_min(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_min(duckdb.unresolved_type, "any", "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE arg_min_null(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE array_agg(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE avg(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bit_and(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bit_or(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bit_xor(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bitstring_agg(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bool_and(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE bool_or(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

-- NOTE: count(*) and count(duckdb.unresolved_type) are already defined in the core

CREATE AGGREGATE favg(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE first(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE fsum(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE geomean(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE histogram(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE histogram(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE histogram_exact(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE last(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE list(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE max(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE max(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE max_by(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE max_by(duckdb.unresolved_type, "any", "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE min(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE min_by(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE min_by(duckdb.unresolved_type, "any", "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE product(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE string_agg(duckdb.unresolved_type, "any") (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);

CREATE AGGREGATE sum(duckdb.unresolved_type) (
    SFUNC = duckdb_unresolved_type_state_trans,
    STYPE = duckdb.unresolved_type,
    FINALFUNC = duckdb_unresolved_type_final
);


-- "AS ASSIGNMENT" cast to boolean for unresolved types, so that they can be
-- used as the final expression in a WHERE clause
CREATE CAST (duckdb.unresolved_type AS boolean)
    WITH INOUT
    AS ASSIGNMENT;

-- Regular casts for all our supported types
-- BOOLEAN (skiping plain boolean because it's right above)
CREATE CAST (duckdb.unresolved_type AS boolean[])
    WITH INOUT;

-- TINYINT (CHAR)
CREATE CAST (duckdb.unresolved_type AS char)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS char[])
    WITH INOUT;

-- SMALLINT (INT2)
CREATE CAST (duckdb.unresolved_type AS smallint)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS smallint[])
    WITH INOUT;

-- INTEGER (INT4)
CREATE CAST (duckdb.unresolved_type AS integer)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS integer[])
    WITH INOUT;

-- BIGINT (INT8)
CREATE CAST (duckdb.unresolved_type AS bigint)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS bigint[])
    WITH INOUT;

-- VARCHAR (BPCHAR, TEXT, VARCHAR)
CREATE CAST (duckdb.unresolved_type AS varchar)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS varchar[])
    WITH INOUT;

-- DATE
CREATE CAST (duckdb.unresolved_type AS date)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS date[])
    WITH INOUT;

-- TIMESTAMP
CREATE CAST (duckdb.unresolved_type AS timestamp)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS timestamp[])
    WITH INOUT;

-- TIMESTAMP WITH TIME ZONE
CREATE CAST (duckdb.unresolved_type AS timestamptz)
    WITH INOUT;

-- FLOAT
CREATE CAST (duckdb.unresolved_type AS real)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS real[])
    WITH INOUT;

-- DOUBLE
CREATE CAST (duckdb.unresolved_type AS double precision)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS double precision[])
    WITH INOUT;

-- NUMERIC (DECIMAL)
CREATE CAST (duckdb.unresolved_type AS numeric)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS numeric[])
    WITH INOUT;

-- UUID
CREATE CAST (duckdb.unresolved_type AS uuid)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS uuid[])
    WITH INOUT;

-- JSON
CREATE CAST (duckdb.unresolved_type AS json)
    WITH INOUT;
CREATE CAST (duckdb.unresolved_type AS json[])
    WITH INOUT;

-- read_parquet function for single path
DROP FUNCTION @extschema@.read_parquet(path text, binary_as_string BOOLEAN,
                                                   filename BOOLEAN,
                                                   file_row_number BOOLEAN,
                                                   hive_partitioning BOOLEAN,
                                                   union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_parquet(path text, binary_as_string BOOLEAN DEFAULT FALSE,
                                                   filename BOOLEAN DEFAULT FALSE,
                                                   file_row_number BOOLEAN DEFAULT FALSE,
                                                   hive_partitioning BOOLEAN DEFAULT FALSE,
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_parquet function for array of paths
DROP FUNCTION @extschema@.read_parquet(path text[], binary_as_string BOOLEAN,
                                                     filename BOOLEAN,
                                                     file_row_number BOOLEAN,
                                                     hive_partitioning BOOLEAN,
                                                     union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_parquet(path text[], binary_as_string BOOLEAN DEFAULT FALSE,
                                                     filename BOOLEAN DEFAULT FALSE,
                                                     file_row_number BOOLEAN DEFAULT FALSE,
                                                     hive_partitioning BOOLEAN DEFAULT FALSE,
                                                     union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_csv function for single path
DROP FUNCTION @extschema@.read_csv(path text, all_varchar BOOLEAN,
                                               allow_quoted_nulls BOOLEAN,
                                               auto_detect BOOLEAN,
                                               auto_type_candidates TEXT[],
                                               compression VARCHAR,
                                               dateformat VARCHAR,
                                               decimal_separator VARCHAR,
                                               delim VARCHAR,
                                               escape VARCHAR,
                                               filename BOOLEAN,
                                               force_not_null TEXT[],
                                               header BOOLEAN,
                                               hive_partitioning BOOLEAN,
                                               ignore_errors BOOLEAN,
                                               max_line_size BIGINT,
                                               names TEXT[],
                                               new_line VARCHAR,
                                               normalize_names BOOLEAN,
                                               null_padding BOOLEAN,
                                               nullstr TEXT[],
                                               parallel BOOLEAN,
                                               quote VARCHAR,
                                               sample_size BIGINT,
                                               sep VARCHAR,
                                               skip BIGINT,
                                               timestampformat VARCHAR,
                                               types TEXT[],
                                               union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_csv(path text, all_varchar BOOLEAN DEFAULT FALSE,
                                               allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                               auto_detect BOOLEAN DEFAULT TRUE,
                                               auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               compression VARCHAR DEFAULT 'auto',
                                               dateformat VARCHAR DEFAULT '',
                                               decimal_separator VARCHAR DEFAULT '.',
                                               delim VARCHAR DEFAULT ',',
                                               escape VARCHAR DEFAULT '"',
                                               filename BOOLEAN DEFAULT FALSE,
                                               force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               header BOOLEAN DEFAULT FALSE,
                                               hive_partitioning BOOLEAN DEFAULT FALSE,
                                               ignore_errors BOOLEAN DEFAULT FALSE,
                                               max_line_size BIGINT DEFAULT 2097152,
                                               names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               new_line VARCHAR DEFAULT '',
                                               normalize_names BOOLEAN DEFAULT FALSE,
                                               null_padding BOOLEAN DEFAULT FALSE,
                                               nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               parallel BOOLEAN DEFAULT FALSE,
                                               quote VARCHAR DEFAULT '"',
                                               sample_size BIGINT DEFAULT 20480,
                                               sep VARCHAR DEFAULT ',',
                                               skip BIGINT DEFAULT 0,
                                               timestampformat VARCHAR DEFAULT '',
                                               types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                               union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_csv function for array of paths
DROP FUNCTION @extschema@.read_csv(path text[], all_varchar BOOLEAN,
                                                  allow_quoted_nulls BOOLEAN,
                                                  auto_detect BOOLEAN,
                                                  auto_type_candidates TEXT[],
                                                  compression VARCHAR,
                                                  dateformat VARCHAR,
                                                  decimal_separator VARCHAR,
                                                  delim VARCHAR,
                                                  escape VARCHAR,
                                                  filename BOOLEAN,
                                                  force_not_null TEXT[],
                                                  header BOOLEAN,
                                                  hive_partitioning BOOLEAN,
                                                  ignore_errors BOOLEAN,
                                                  max_line_size BIGINT,
                                                  names TEXT[],
                                                  new_line VARCHAR,
                                                  normalize_names BOOLEAN,
                                                  null_padding BOOLEAN,
                                                  nullstr TEXT[],
                                                  parallel BOOLEAN,
                                                  quote VARCHAR,
                                                  sample_size BIGINT,
                                                  sep VARCHAR,
                                                  skip BIGINT,
                                                  timestampformat VARCHAR,
                                                  types TEXT[],
                                                  union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_csv(path text[], all_varchar BOOLEAN DEFAULT FALSE,
                                                  allow_quoted_nulls BOOLEAN DEFAULT TRUE,
                                                  auto_detect BOOLEAN DEFAULT TRUE,
                                                  auto_type_candidates TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  compression VARCHAR DEFAULT 'auto',
                                                  dateformat VARCHAR DEFAULT '',
                                                  decimal_separator VARCHAR DEFAULT '.',
                                                  delim VARCHAR DEFAULT ',',
                                                  escape VARCHAR DEFAULT '"',
                                                  filename BOOLEAN DEFAULT FALSE,
                                                  force_not_null TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  header BOOLEAN DEFAULT FALSE,
                                                  hive_partitioning BOOLEAN DEFAULT FALSE,
                                                  ignore_errors BOOLEAN DEFAULT FALSE,
                                                  max_line_size BIGINT DEFAULT 2097152,
                                                  names TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  new_line VARCHAR DEFAULT '',
                                                  normalize_names BOOLEAN DEFAULT FALSE,
                                                  null_padding BOOLEAN DEFAULT FALSE,
                                                  nullstr TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  parallel BOOLEAN DEFAULT FALSE,
                                                  quote VARCHAR DEFAULT '"',
                                                  sample_size BIGINT DEFAULT 20480,
                                                  sep VARCHAR DEFAULT ',',
                                                  skip BIGINT DEFAULT 0,
                                                  timestampformat VARCHAR DEFAULT '',
                                                  types TEXT[] DEFAULT ARRAY[]::TEXT[],
                                                  union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- iceberg_scan function
DROP FUNCTION @extschema@.iceberg_scan(path text, allow_moved_paths BOOLEAN,
                                                   mode TEXT,
                                                   metadata_compression_codec TEXT,
                                                   skip_schema_inference BOOLEAN,
                                                   version TEXT,
                                                   version_name_format TEXT);
CREATE FUNCTION @extschema@.iceberg_scan(path text, allow_moved_paths BOOLEAN DEFAULT FALSE,
                                                   mode TEXT DEFAULT '',
                                                   metadata_compression_codec TEXT DEFAULT 'none',
                                                   skip_schema_inference BOOLEAN DEFAULT FALSE,
                                                   version TEXT DEFAULT 'version-hint.text',
                                                   version_name_format TEXT DEFAULT 'v%s%s.metadata.json,%s%s.metadata.json')
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- delta_scan function
DROP FUNCTION @extschema@.delta_scan(path text);
CREATE FUNCTION @extschema@.delta_scan(path text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_json function for single path
DROP FUNCTION @extschema@.read_json(path text, auto_detect BOOLEAN,
                                                 compression VARCHAR,
                                                 dateformat VARCHAR,
                                                 format VARCHAR,
                                                 ignore_errors BOOLEAN,
                                                 maximum_depth BIGINT,
                                                 maximum_object_size INT,
                                                 records VARCHAR,
                                                 sample_size BIGINT,
                                                 timestampformat VARCHAR,
                                                 union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_json(path text, auto_detect BOOLEAN DEFAULT FALSE,
                                                 compression VARCHAR DEFAULT 'auto',
                                                 dateformat VARCHAR DEFAULT 'iso',
                                                 format VARCHAR DEFAULT 'array',
                                                 ignore_errors BOOLEAN DEFAULT FALSE,
                                                 maximum_depth BIGINT DEFAULT -1,
                                                 maximum_object_size INT DEFAULT 16777216,
                                                 records VARCHAR DEFAULT 'records',
                                                 sample_size BIGINT DEFAULT 20480,
                                                 timestampformat VARCHAR DEFAULT 'iso',
                                                 union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

-- read_json function for array of paths
DROP FUNCTION @extschema@.read_json(path text[], auto_detect BOOLEAN,
                                                   compression VARCHAR,
                                                   dateformat VARCHAR,
                                                   format VARCHAR,
                                                   ignore_errors BOOLEAN,
                                                   maximum_depth BIGINT,
                                                   maximum_object_size INT,
                                                   records VARCHAR,
                                                   sample_size BIGINT,
                                                   timestampformat VARCHAR,
                                                   union_by_name BOOLEAN);
CREATE FUNCTION @extschema@.read_json(path text[], auto_detect BOOLEAN DEFAULT FALSE,
                                                   compression VARCHAR DEFAULT 'auto',
                                                   dateformat VARCHAR DEFAULT 'iso',
                                                   format VARCHAR DEFAULT 'array',
                                                   ignore_errors BOOLEAN DEFAULT FALSE,
                                                   maximum_depth BIGINT DEFAULT -1,
                                                   maximum_object_size INT DEFAULT 16777216,
                                                   records VARCHAR DEFAULT 'records',
                                                   sample_size BIGINT DEFAULT 20480,
                                                   timestampformat VARCHAR DEFAULT 'iso',
                                                   union_by_name BOOLEAN DEFAULT FALSE)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;

CREATE FUNCTION duckdb.query(query text)
RETURNS SETOF duckdb.row
SET search_path = pg_catalog, pg_temp
AS 'MODULE_PATHNAME', 'duckdb_only_function'
LANGUAGE C;
