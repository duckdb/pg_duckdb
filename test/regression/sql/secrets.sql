
SET duckdb.force_execution TO false;

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

SELECT last_value FROM duckdb.secrets_table_seq;

-- INSERT SHOULD TRIGGER UPDATE OF SECRETS

INSERT INTO duckdb.secrets (type, key_id, secret, session_token, region)
VALUES ('S3', 'access_key_id_1', 'secret_access_key', 'session_token', 'us-east-1');

SELECT last_value FROM duckdb.secrets_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

INSERT INTO duckdb.secrets (type, key_id, secret, session_token, region)
VALUES ('S3', 'access_key_id_2', 'secret_access_key', 'session_token', 'us-east-1');

SELECT last_value FROM duckdb.secrets_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

-- DELETE SHOULD TRIGGER UPDATE OF SECRETS
DELETE FROM duckdb.secrets WHERE key_id = 'access_key_id_1';

SELECT last_value FROM duckdb.secrets_table_seq;

SELECT * FROM duckdb.raw_query($$ SELECT name FROM duckdb_secrets() $$);

SET duckdb.force_execution TO true;
