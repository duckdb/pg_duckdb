SET duckdb.force_execution TO false;

-- MD is not enabled
SELECT * FROM duckdb.is_motherduck_enabled();

-- Must provide a TYPE clause
CREATE SERVER invalid_server1 FOREIGN DATA WRAPPER pg_duckdb;

-- Must be a valid TYPE
CREATE SERVER invalid_server2
TYPE 'foo'
FOREIGN DATA WRAPPER pg_duckdb;

-- Should succeed with no option
CREATE SERVER valid_md_server1
TYPE 'motherduck'
FOREIGN DATA WRAPPER pg_duckdb;

-- MD is still NOT enabled
SELECT * FROM duckdb.is_motherduck_enabled();

-- Should create a USER MAPPING
CREATE USER user1 IN ROLE duckdb_group;

-- Missing token
CREATE USER MAPPING FOR user1
SERVER valid_md_server1;

-- Invalid option
CREATE USER MAPPING FOR user1
SERVER valid_md_server1
OPTIONS (invalid_option 'foo');

-- Good mapping
CREATE USER MAPPING FOR user1
SERVER valid_md_server1
OPTIONS (token 'foo');

-- Can't have two mappings for the same (user, server)
CREATE USER MAPPING FOR user1
SERVER valid_md_server1
OPTIONS (token 'foo');

-- MD is still not enabled (no mapping for current user)
SELECT * FROM duckdb.is_motherduck_enabled();

-- Mapping for current user
CREATE USER MAPPING FOR CURRENT_USER
SERVER valid_md_server1
OPTIONS (token 'foo');

-- Now MD is enabled
SELECT * FROM duckdb.is_motherduck_enabled();

-- Drop server
DROP SERVER valid_md_server1 CASCADE;

-- Now MD is not enabled anymore
SELECT * FROM duckdb.is_motherduck_enabled();

-- Use helper to enable MD: will fail since there's no token in environment
SELECT duckdb.enable_motherduck();

-- Use helper to enable MD: will succeed
SELECT duckdb.enable_motherduck('foo');

-- Now MD is enabled again
SELECT * FROM duckdb.is_motherduck_enabled();

-- Drop user mapping
DROP USER MAPPING FOR CURRENT_USER SERVER md_server;

-- Now MD is not enabled anymore
SELECT * FROM duckdb.is_motherduck_enabled();

-- TODO: test ALTER SERVER & USER MAPPING
