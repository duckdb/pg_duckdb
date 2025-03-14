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

CREATE USER MAPPING FOR user1 SERVER valid_md_server1;

-- Now MD is enabled
SELECT * FROM duckdb.is_motherduck_enabled();

-- TODO: test ALTER & DROP SERVER
-- TODO: test ALTER & DROP USER MAPPING
