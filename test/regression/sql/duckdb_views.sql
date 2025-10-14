CREATE USER duckdb_view_admin IN ROLE duckdb_group;
CREATE USER duckdb_view_user1;

SET ROLE duckdb_view_admin;

CREATE VIEW duckdb_view AS SELECT * FROM read_parquet('/nonexisting.parquet');

SET ROLE duckdb_view_user1;

SELECT * from duckdb_view LIMIT 1;

SET ROLE duckdb_view_admin;
GRANT SELECT ON duckdb_view TO duckdb_view_user1;
SET ROLE duckdb_view_user1;

SELECT * from duckdb_view LIMIT 1;
