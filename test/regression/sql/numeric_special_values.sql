-- Postgres stores NaN and +/-Infinity as "special" NUMERICs, which have no
-- digits. Converting those to a DuckDB DECIMAL used to silently produce 0,
-- which corrupted the result of any query touching such a row.

CREATE TABLE special_numerics (
    id       int,
    dec      numeric(10,2),          -- becomes a DuckDB DECIMAL
    unbound  numeric,                -- unsupported by DuckDB unless converted to DOUBLE
    f4       real,
    f8       double precision
);

-- Note that a constrained NUMERIC can hold NaN, but not +/-Infinity.
INSERT INTO special_numerics VALUES
    (1, 'NaN', 'NaN',       'NaN',       'NaN'),
    (2, 10.00, 10,          10,          10),
    (3, NULL,  'Infinity',  'Infinity',  'Infinity'),
    (4, NULL,  '-Infinity', '-Infinity', '-Infinity');

-- The floating point types round-trip through DuckDB unchanged.
SET duckdb.force_execution = true;
SELECT id, f4, f8 FROM special_numerics ORDER BY id;

-- A DuckDB DECIMAL is a fixed point integer and cannot represent NaN, so we
-- error out instead of returning a wrong number.
SELECT id, dec FROM special_numerics ORDER BY id;
SELECT sum(dec), avg(dec), min(dec), max(dec) FROM special_numerics;

-- Rows without a special value are unaffected.
SELECT sum(dec), min(dec) FROM special_numerics WHERE id = 2;

-- Postgres itself has no trouble with them.
SET duckdb.force_execution = false;
SELECT id, dec FROM special_numerics ORDER BY id;
SELECT sum(dec), avg(dec), min(dec), max(dec) FROM special_numerics;

-- A DOUBLE can represent all three special values, so converting an
-- unconstrained NUMERIC to one is lossless.
SET duckdb.force_execution = true;
SET duckdb.convert_unsupported_numeric_to_double = true;
SELECT id, unbound FROM special_numerics ORDER BY id;
SELECT id, unbound = 'NaN'::float8 AS is_nan FROM special_numerics WHERE id = 1;

DROP TABLE special_numerics;
