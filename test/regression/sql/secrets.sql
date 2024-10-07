
SET duckdb.execution TO false;

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

-- INSERT SHOULD TRIGGER UPDATE OF SECRETS

INSERT INTO duckdb.secrets (type, id, secret, session_token, region)
VALUES ('S3', 'access_key_id_1', 'secret_access_key', 'session_token', 'us-east-1');

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

INSERT INTO duckdb.secrets (type, id, secret, session_token, region)
VALUES ('S3', 'access_key_id_2', 'secret_access_key', 'session_token', 'us-east-1');

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

-- DELETE SHOULD TRIGGER UPDATE OF SECRETS
DELETE FROM duckdb.secrets WHERE id = 'access_key_id_1';

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

SET duckdb.execution TO true;
