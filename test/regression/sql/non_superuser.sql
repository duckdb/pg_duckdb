CREATE USER user1 IN ROLE duckdb_group;
CREATE USER user2 IN ROLE duckdb_group;
CREATE USER user3;
CREATE TABLE t (a int);
GRANT SELECT ON t TO user1;
GRANT SELECT ON t TO user3;
-- Should be allowed because access was granted
SET ROLE user1;
SELECT * FROM t;
-- Should fail
SET ROLE user2;
SELECT * FROM t;

-- Should fail because we're not allowed to read the internal tables by default
SELECT * from duckdb.secrets;
SELECT * from duckdb.tables;
SELECT * from duckdb.extensions;

-- Should fail because any Postgres tables accesesd from DuckDB will have their
-- permissions checked even if it happens straight from DuckDB.
SET duckdb.force_execution = false;
SELECT * FROM duckdb.raw_query($$ SELECT * FROM pgduckdb.public.t $$);
SET duckdb.force_execution = true;

-- read_csv from the local filesystem should be disallowed
SELECT count(r['sepal.length']) FROM read_csv('../../data/iris.csv') r;
-- Should fail because DuckDB execution is not allowed for this user
SET ROLE user3;
SELECT * FROM t;

-- And all these duckdb functions should also fail, even the ones that don't
-- actually open a duckdb connection.
SET duckdb.force_execution = false;
SELECT * FROM duckdb.raw_query($$ SELECT * FROM pgduckdb.public.t $$);
CALL duckdb.recycle_ddb();
SET duckdb.force_execution = true;

-- Should work with regular posgres execution though, because this user is
-- allowed to read the table.
SET duckdb.force_execution = false;
SELECT * FROM t;
SET duckdb.force_execution = true;

-- Let's add RLS
RESET ROLE;
ALTER TABLE t ENABLE ROW LEVEL SECURITY;
-- Should still be allowed, we're superuser
SELECT * FROM t;

-- Should fall back to PG execution, because we don't support RLS
SET ROLE user1;
SELECT * FROM t;

-- Should fail because we require duckdb execution so no fallback
SELECT public.approx_count_distinct(a) FROM t;

SET duckdb.force_execution = false;
SELECT * FROM duckdb.raw_query($$ SELECT * FROM pgduckdb.public.t $$);
SET duckdb.force_execution = true;


-- Extension installation
SET duckdb.force_execution = false;
-- Should fail because installing extensions is restricted for super users by default
SELECT * FROM duckdb.install_extension('iceberg');
-- Similarly when trying using raw duckdb commands
SELECT * FROM duckdb.raw_query($$ INSTALL someextension $$);
SET duckdb.force_execution = true;


-- It should be possible to install extensions as non-superuser after the
-- following grants.
RESET ROLE;
GRANT ALL ON FUNCTION duckdb.install_extension(TEXT, TEXT) TO user1;
GRANT ALL ON TABLE duckdb.extensions TO user1;
GRANT ALL ON SEQUENCE duckdb.extensions_table_seq TO user1;

-- You need to reconnect though (or run recycle_ddb), because
-- disabled_filesystems cannot be changed after it has been set.
\c
SET ROLE user1;
SET duckdb.force_execution = false;
SELECT * FROM duckdb.install_extension('iceberg');
-- We should handle SQL injections carefully though to only allow INSTALL
SELECT * FROM duckdb.install_extension($$ '; select * from hacky '' $$);
INSERT INTO duckdb.extensions (name) VALUES ($$ '; select * from hacky $$);
SELECT * FROM duckdb.query($$ SELECT 1 $$);
TRUNCATE duckdb.extensions;
SET duckdb.force_execution = true;

RESET ROLE;
DROP TABLE t;
DROP OWNED BY user1;
DROP USER user1, user2, user3;
