LOAD 'quack';

CREATE OR REPLACE FUNCTION read_parquet(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_parquet(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_parquet(path text[])
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_parquet(TEXT[])` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_csv(path text)
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_csv(TEXT)` only works with Duckdb execution.';
END;
$func$;

CREATE OR REPLACE FUNCTION read_csv(path text[])
RETURNS SETOF record LANGUAGE 'plpgsql' AS
$func$
BEGIN
   RAISE EXCEPTION 'Function `read_csv(TEXT[])` only works with Duckdb execution.';
END;
$func$;

CREATE SCHEMA quack;
SET search_path TO quack;

CREATE TABLE secrets (
   type TEXT NOT NULL,
   id TEXT NOT NULL,
   secret TEXT NOT NULL,
   region TEXT,
   endpoint TEXT,
   r2_account_id TEXT,
   CONSTRAINT type_constraint CHECK (type IN ('S3', 'GCS', 'R2'))
);

CREATE OR REPLACE FUNCTION quack_secret_r2_check()
RETURNS TRIGGER AS
$$
BEGIN
   IF NEW.type = 'R2' AND NEW.r2_account_id IS NULL THEN 
      Raise Exception '`R2` cloud type secret requires valid `r2_account_id` column value';
   END IF;
   RETURN NEW;
END;
$$ LANGUAGE PLpgSQL;

CREATE TRIGGER quack_secret_r2_tr BEFORE INSERT OR UPDATE ON secrets
FOR EACH ROW EXECUTE PROCEDURE quack_secret_r2_check();

RESET search_path;
