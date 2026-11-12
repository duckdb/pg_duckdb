# Secrets

DuckDB secrets can be configured either using utility functions or with a Foreign Data Wrapper for more advanced cases.

## Quick Start: Simple Secrets

The easiest way to configure credentials is using the utility functions:

### AWS S3 / Compatible Storage

```sql
-- Basic S3 secret (most common)
SELECT duckdb.create_simple_secret(
    type := 'S3',
    key_id := 'your_access_key_id',
    secret := 'your_secret_access_key',
    region := 'us-east-1'
);
```


There are many more arguments to this function:

```sql
SELECT duckdb.create_simple_secret(
    type          := 'S3',          -- Type: one of (S3, GCS, R2)
    key_id        := 'access_key_id',
    secret        := 'xxx',
    session_token := 'yyy',         -- (optional)
    region        := 'us-east-1',   -- (optional)
    url_style     := 'xxx',         -- (optional)
    provider      := 'xxx',         -- (optional)
    endpoint      := 'xxx',         -- (optional)
    scope         := 'xxx',         -- (optional)
    validation    := 'xxx',         -- (optional)
    use_ssl       := 'xxx'          -- (optional)
)
```

For Azure secrets you may use:
```sql
SELECT duckdb.create_azure_secret(
    '< connection string >',
    scope := 'xxx'          -- (optional)
);
```

## Secrets with `credential_chain` provider:

For more advanced use-cases, one can define secrets with a `SERVER` (and `USER MAPPING`) on `duckdb` Foreign Data Wrapper:

```sql
CREATE SERVER my_s3_secret
TYPE 's3'
FOREIGN DATA WRAPPER duckdb
OPTIONS (PROVIDER 'credential_chain');
```

## Secrets with `secret_access_key`:

When your secret contains sensitive information, you need to create an additional `USER MAPPING` like this:

```sql
CREATE SERVER my_s3_secret TYPE 's3' FOREIGN DATA WRAPPER duckdb;

CREATE USER MAPPING FOR CURRENT_USER SERVER my_s3_secret
OPTIONS (KEY_ID 'my_secret_key', SECRET 'my_secret_value');
```

You may use any of the supported DuckDB secret type as long as the related extension is installed.
Please refer to this page for more: https://duckdb.org/docs/stable/configuration/secrets_manager.html

## How it works

Secrets are stored in a combination of `SERVER` and `USER MAPPING` on `duckdb` Foreign Data Wrapper. The `USER MAPPING` hosts the sensitive elements like `token`, `session_token` and `secret`.
Each time a DuckDB instance is created by pg_duckdb, and when a secret is modified, the secrets are loaded into the DuckDB secrets manager as non-persistent secrets.

## Security Considerations

**Important:** Do not grant `USAGE` permission on the `duckdb` foreign data wrapper to regular users.

The owner of a foreign server can create user mappings for that server for **any user**, so only grant this to access to administrative users. Otherwise a regular user could create secrets for certain scopes for unsuspecting users.

### Secrets in logs and query statistics

Postgres can write the text of a statement to the server log, for example as the `STATEMENT:` of any statement that fails, or for every statement with `log_statement = 'all'`. Extensions like `pg_stat_statements` also store the text of utility statements as typed. To keep secrets out of these, pg_duckdb:

* redacts statements written to the server log, unless [`duckdb.allow_unredacted_secrets`](settings.md#duckdballow_unredacted_secrets) is enabled. If a statement contains one of the words `connection_string`, `key_id`, `mapping`, `motherduck`, `password`, `secret` or `token` (in any case, and not inside a string literal quoted with `'...'` or `$$ ... $$`, except for strings that contain code: the code of a `DO` statement, a string right after `AS` like a function body, and the arguments of `duckdb.raw_query()` and `duckdb.query()`), everything after the first of them is replaced with `<redacted by pg_duckdb because it may contain secrets, see duckdb.allow_unredacted_secrets option>`. The same is done for the log messages of `log_statement` and `log_min_duration_statement`, syntax errors in such a statement, statements run inside functions that show up in the error context, and statements of other sessions listed in a deadlock report. If such a statement passes values as bind parameters, the logged parameter values are redacted too.
* hides `CREATE/ALTER USER MAPPING` statements for `duckdb` servers and `CALL duckdb.enable_motherduck(...)` from `pg_stat_statements`, unless `duckdb.allow_unredacted_secrets` is enabled.

The redaction also cuts statements that contain no secrets but use one of these words, for example a column named `token`. It is best effort, so some secrets are not redacted: code in other string literals (for example `dblink_exec('CREATE USER MAPPING ...')`), statements that pass secrets without any of these words (for example `EXECUTE` of a prepared statement, a function that calls the secret helpers, or names written with Unicode escapes like `duckdb.U&"create_simple_s\0065cret"(...)`), error messages of DuckDB that quote the statement, pg_duckdb's own `DEBUG` messages (shown with `log_min_messages = debug1` or lower), which include the queries it sends to DuckDB, secrets that are built dynamically, and bound parameter values logged with `log_parameter_max_length_on_error`. Extensions other than `pg_stat_statements` that record statement text may still store secrets. While a statement runs, `pg_stat_activity` shows its full text to superusers and members of `pg_read_all_stats`. Prefer setting secrets from a session with statement logging disabled, and treat server logs as sensitive.

URLs are not redacted, so do not put credentials in them, neither in user info like `https://user:password@example.com/file.parquet` nor in parameters like `s3://bucket/file.parquet?s3_access_key_id=...&s3_secret_access_key=...`. DuckDB's error messages quote such URLs too. Use secrets instead.

## Further reading

* [DuckDB Secrets Manager](https://duckdb.org/docs/configuration/secrets_manager.html)
* [S3 API Support](https://duckdb.org/docs/extensions/httpfs/s3api.html)
* [Google Cloud Storage Import](https://duckdb.org/docs/guides/network_cloud_storage/gcs_import.html)
* [Cloudflare R2 Import](https://duckdb.org/docs/guides/network_cloud_storage/cloudflare_r2_import.html)
* [Azure Extension](https://duckdb.org/docs/extensions/azure)
