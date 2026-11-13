-- With duckdb.force_execution, queries that call a Postgres catalog function
-- missing from DuckDB in their FROM clause fall back to Postgres without a
-- warning (#881).
CREATE FUNCTION issue_881_trigger() RETURNS event_trigger LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
	FOR r IN SELECT upper(object_type) AS object_type, object_identity, upper(command_tag) AS command_tag
	         FROM pg_event_trigger_ddl_commands() LOOP
		RAISE NOTICE 'ddl: % % %', r.command_tag, r.object_type, r.object_identity;
	END LOOP;
END $$;
CREATE EVENT TRIGGER issue_881 ON ddl_command_end EXECUTE FUNCTION issue_881_trigger();

CREATE TABLE issue_881_sales AS SELECT i FROM generate_series(1, 3) i;
CREATE TEMP TABLE issue_881_temp_sales USING duckdb AS SELECT * FROM issue_881_sales;
SELECT count(*) FROM issue_881_temp_sales;

DROP EVENT TRIGGER issue_881;
DROP FUNCTION issue_881_trigger;

-- Also in a subquery
SELECT count(*) FROM (SELECT * FROM pg_get_keywords()) k WHERE word = 'select';

-- Functions outside of pg_catalog that DuckDB does not have still warn
CREATE FUNCTION issue_881_srf() RETURNS SETOF int LANGUAGE sql AS 'SELECT 1';
SELECT * FROM issue_881_srf();
DROP FUNCTION issue_881_srf;

-- pg_catalog functions that DuckDB has still run in DuckDB
SELECT pg_typeof(g) FROM generate_series(1, 2) g LIMIT 1;

DROP TABLE issue_881_temp_sales, issue_881_sales;
