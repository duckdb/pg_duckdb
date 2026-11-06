-- Verify INSERT into wide DECIMAL columns works (previously failed because
-- the deparse emitted "::numeric" which DuckDB mapped to DECIMAL(18,3)).
CREATE TEMP TABLE t(n numeric(31,3)) USING duckdb;
INSERT INTO t VALUES (1.7820000000000002e+16);
INSERT INTO t VALUES (12345.678);
INSERT INTO t VALUES (-99999999999999.999);
SELECT * FROM t ORDER BY n;
DROP TABLE t;
