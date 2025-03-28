-- Invalid type
CREATE SERVER invalid_duckdb_server
TYPE 'unknown'
FOREIGN DATA WRAPPER duckdb;

-- Invalid use of restricted option (with various casing)
CREATE SERVER invalid_duckdb_server
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (tOkeN 'very secret');

CREATE SERVER invalid_duckdb_server
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (SECRET 'dont leak me');

CREATE SERVER invalid_duckdb_server
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (session_TOKEN 'shhhhh');

CREATE SERVER invalid_duckdb_server
TYPE 'azure'
FOREIGN DATA WRAPPER duckdb
OPTIONS (CONNECTION_STRING 'all my life secrets here');

-- No secret was created
SELECT * FROM duckdb.query($$ SELECT count(*) FROM duckdb_secrets(); $$);

-- Valid S3
CREATE SERVER valid_s3_server
TYPE 's3'
FOREIGN DATA WRAPPER duckdb;

-- Secret was created
SELECT * FROM duckdb.query($$ FROM which_secret('s3://some-bucket/file.parquet', 's3'); $$);

-- Valid secrets for other types (don't load Azure or other extensions)
CREATE SERVER valid_r2_server TYPE 'r2' FOREIGN DATA WRAPPER duckdb;
CREATE SERVER valid_hf_server TYPE 'huggingface' FOREIGN DATA WRAPPER duckdb;
CREATE SERVER valid_gcs_server TYPE 'gcs' FOREIGN DATA WRAPPER duckdb;

-- Check them all
SELECT * FROM duckdb.query($$ SELECT name, type FROM duckdb_secrets(); $$);

-- Add one more (test drop & recreate)
CREATE SERVER valid_http_server TYPE 'http' FOREIGN DATA WRAPPER duckdb;

-- And verify we have them all
SELECT * FROM duckdb.query($$ SELECT name, type FROM duckdb_secrets(); $$);

-- PROVIDER option needs the `aws` extension
SELECT duckdb.install_extension('aws');

CREATE SERVER valid_s3_cred_chain
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (PROVIDER 'credential_chain', CHAIN ''); -- use empty chain otherwise it takes too much time

-- Drop some
DROP SERVER valid_r2_server;
DROP SERVER valid_hf_server;
DROP SERVER valid_gcs_server;
DROP SERVER valid_http_server;

-- Make sure we have the 'credential_chain'
SELECT * FROM duckdb.query($$
    SELECT
        name,
        map_from_entries(
            list_transform( -- split 'key=value' strings to have an array of [key, value]
            list_transform( -- split the secret string by `;` to have 'key=value' strings
                regexp_split_to_array(secret_string, ';'),
                x -> regexp_split_to_array(x, '=')
            ),
            x -> struct_pack(k := x[1], v := x[2])
            )
        ).provider as provider
    FROM duckdb_secrets();
$$);

DROP SERVER valid_s3_server;
DROP SERVER valid_s3_cred_chain;

-- Nothing
SELECT * FROM duckdb.query($$ SELECT name, type FROM duckdb_secrets(); $$);

-- Now create secrets with USER MAPPING
CREATE SERVER valid_s3_server TYPE 's3' FOREIGN DATA WRAPPER duckdb;
CREATE USER MAPPING FOR CURRENT_USER
SERVER valid_s3_server
OPTIONS (KEY_ID 'my_secret_key', SECRET 'my_secret_value');

SELECT * FROM duckdb.query($$
    SELECT
        name,
        secrets.key_id,
        secrets.secret
    FROM (
        SELECT
            name,
            map_from_entries(list_transform(list_transform(regexp_split_to_array(secret_string, ';'),x -> regexp_split_to_array(x, '=')),x -> struct_pack(k := x[1], v := x[2]))) as secrets
        FROM duckdb_secrets()
    );
$$);

SET client_min_messages=WARNING; -- suppress NOTICE that include username
DROP SERVER valid_s3_server CASCADE;

-- Nothing
SELECT * FROM duckdb.query($$ SELECT name, type FROM duckdb_secrets(); $$);
