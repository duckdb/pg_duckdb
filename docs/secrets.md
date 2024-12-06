# Secrets

DuckDB secrets can be configured in the `duckdb.secrets` table. For example:

```sql
-- Session Token is Optional
INSERT INTO duckdb.secrets
(type, key_id, secret, session_token, region)
VALUES ('S3', 'access_key_id', 'secret_access_key', 'session_token', 'us-east-1');
```

## Columns

| Name | Type | Required | Description |
| :--- | :--- | :------- | :---------- |
| name | text | no | automatically generated UUID (primary key) |
| type | text | yes | One of `S3` for Amazon S3, `GCS` for Google Cloud Storage, `R2` for Cloudflare R2, or `Azure` for Azure Blob Storage. |
| key_id | text | yes | the "ID" portion of the secret |
| secret | text | yes | the "password" portion of the secret |
| session_token | text | no | the AWS S3 session token if required for your credential |
| region | text | S3 only | for AWS S3, this specifies the region of your bucket |
| endpoint | text | no | if using an S3-compatible service other than AWS, this specifies the endpoint of the service |
| r2_account_id | text | R2 only | if using Cloudflare R2, the account ID for the credential |
| use_ssl | boolean | no | `true` by default; `false` is principally for use with custom minio configurations |
| scope | text | no | The URL prefix which applies to this credential. This is used to [select between multiple credentials](scope) for the same service. |

[scope]: https://duckdb.org/docs/configuration/secrets_manager.html#creating-multiple-secrets-for-the-same-service-type

## How it works

Secrets are stored in a Postgres heap table. Each time a DuckDB instance is created by pg_duckdb, and when a secret is modified, the secrets are loaded into the DuckDB secrets manager as non-persistent secrets.

## Caveats

* Only the listed types of secrets above are currently supported. As of DuckDB 1.1.3, MySQL, Huggingface, and PostgreSQL secrets are not supported.
* `CHAIN` authentication is not supported.

## Further reading

* [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html)
* [S3 API Support](https://duckdb.org/docs/extensions/httpfs/s3api.html)
