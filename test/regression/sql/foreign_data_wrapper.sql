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

-- Now MD is enabled
SELECT * FROM duckdb.is_motherduck_enabled();

-- TODO: test ALTER & DROP SERVER
-- TODO: test ALTER & DROP USER MAPPING
