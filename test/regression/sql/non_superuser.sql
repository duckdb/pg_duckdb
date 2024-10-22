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

-- Should fail because raw_query is to dangerous for regular users
SET duckdb.force_execution = false;
SELECT * FROM duckdb.raw_query($$ SELECT * FROM t $$);
SELECT * FROM duckdb.install_extension('some hacky sql');
SET duckdb.force_execution = true;

-- Should fail because DuckDB execution is not allowed for this user
SET ROLE user3;
SELECT * FROM t;

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

-- Should fail now, we don't support RLS
SET ROLE user1;
SELECT * FROM t;

RESET ROLE;
DROP TABLE t;
DROP USER user1, user2;
