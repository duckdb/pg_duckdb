-- An aborted transaction or statement should not leave an open DuckDB result
-- behind, which would make all later DuckDB queries in the session fail.
-- A cursor over a streamed result, ended by ROLLBACK without closing it
BEGIN;
DECLARE c CURSOR FOR SELECT r['utinyint']::int FROM read_parquet('../../data/unsigned_types.parquet') r;
FETCH 1 FROM c;
ROLLBACK;
SELECT count(*) FROM read_parquet('../../data/unsigned_types.parquet');
-- The same, but the transaction aborts because of an error
BEGIN;
DECLARE c CURSOR FOR SELECT r['utinyint']::int FROM read_parquet('../../data/unsigned_types.parquet') r;
FETCH 1 FROM c;
SELECT 1/0;
ROLLBACK;
SELECT count(*) FROM read_parquet('../../data/unsigned_types.parquet');
-- An error while a PL/pgSQL loop iterates over a streamed result
DO $$
DECLARE
	r record;
BEGIN
	FOR r IN SELECT * FROM duckdb.query($q$ SELECT * FROM range(100000) $q$) LOOP
		RAISE EXCEPTION 'stop';
	END LOOP;
END $$;
SELECT count(*) FROM read_parquet('../../data/unsigned_types.parquet');
-- A cursor that is still open at COMMIT
BEGIN;
DECLARE c CURSOR FOR SELECT r['utinyint']::int FROM read_parquet('../../data/unsigned_types.parquet') r;
FETCH 1 FROM c;
COMMIT;
SELECT count(*) FROM read_parquet('../../data/unsigned_types.parquet');
